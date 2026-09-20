# SPDX-License-Identifier: Apache-2.0
# SPDX-FileCopyrightText: Copyright The Lance Authors

"""Manifest slicing for distributed tasks.

Opening a Lance dataset decodes its whole manifest, which on large tables is
dominated by the fragment list and can reach gigabytes. A distributed task only
needs the manifest header (schema with field ids, storage format, config, base
paths) plus the fragments it works on.

A serialized manifest is a ``lance.table.Manifest`` protobuf message whose
field 2 is ``repeated DataFragment fragments``. Protobuf repeated fields are
plain concatenations of records and top-level fields may appear in any order,
so the header bytes followed by a subset of the raw fragment records is itself
a valid manifest. Workers open it with
``LanceDataset(uri, version=V, serialized_manifest=...)`` and see a dataset
that contains only those fragments.

This only applies to manifests that keep the fragment list inline. Datasets
whose fragment metadata lives outside the manifest must use
``manifest_mode="full"``.
"""

from collections.abc import Iterable
from typing import TYPE_CHECKING, Literal

if TYPE_CHECKING:
    import lance

ManifestMode = Literal["full", "slice"]

# Field numbers in lance/protos/table.proto.
_MANIFEST_FRAGMENTS_FIELD = 2
_DATA_FRAGMENT_ID_KEY = (1 << 3) | 0  # field 1, varint


def _read_varint(buf: memoryview, pos: int) -> tuple[int, int]:
    result = shift = 0
    while True:
        byte = buf[pos]
        pos += 1
        result |= (byte & 0x7F) << shift
        if not byte & 0x80:
            return result, pos
        shift += 7


class ManifestSlicer:
    """Split a serialized manifest into its header and fragment records.

    Built on the driver, which holds the full manifest anyway. The slices it
    produces are small enough to ship with every task.
    """

    def __init__(self, manifest: bytes):
        buf = memoryview(manifest)
        header = []
        self._buf = buf
        self._fragments: dict[int, tuple[int, int]] = {}

        pos = 0
        while pos < len(buf):
            start = pos
            key, pos = _read_varint(buf, pos)
            field, wire_type = key >> 3, key & 7
            if wire_type == 0:
                _, pos = _read_varint(buf, pos)
            elif wire_type == 1:
                pos += 8
            elif wire_type == 5:
                pos += 4
            elif wire_type == 2:
                length, pos = _read_varint(buf, pos)
                body, pos = pos, pos + length
                if field == _MANIFEST_FRAGMENTS_FIELD:
                    # proto3 omits the id when it is 0.
                    fragment_id = 0
                    if length:
                        id_key, id_pos = _read_varint(buf, body)
                        if id_key == _DATA_FRAGMENT_ID_KEY:
                            fragment_id, _ = _read_varint(buf, id_pos)
                    # Also trips if an encoder ever stops writing the id first.
                    if fragment_id in self._fragments:
                        raise ValueError(f"Duplicate fragment id {fragment_id}")
                    self._fragments[fragment_id] = (start, pos)
                    continue
            else:
                raise ValueError(f"Unexpected protobuf wire type {wire_type}")
            header.append(bytes(buf[start:pos]))

        self.header = b"".join(header)

    def fragments(self, fragment_ids: Iterable[int]) -> bytes:
        """Raw manifest records of the given fragments, in fragment id order."""
        return b"".join(
            self._buf[start:end]
            for start, end in (self._fragments[i] for i in sorted(set(fragment_ids)))
        )

    def slice(self, fragment_ids: Iterable[int]) -> bytes:
        """A serialized manifest containing the header and the given fragments."""
        return self.header + self.fragments(fragment_ids)


def _max_schema_field_id(fields) -> int:
    return max(
        (max(f.id(), _max_schema_field_id(f.children())) for f in fields), default=-1
    )


def field_id_witness(dataset: "lance.LanceDataset") -> list[int]:
    """Fragments a column-adding slice must include to keep field ids unique.

    New field ids are allocated above the manifest's ``max_field_id``, which
    also counts ids that dropped columns left behind in data files. When such
    ids exist only in some fragments, a slice without them would reuse an id
    and read the dropped column's data as the new column. Including one
    fragment that holds the table-wide maximum keeps the slice's value equal to
    the full manifest's.
    """
    full_max = dataset._ds.max_field_id
    if _max_schema_field_id(dataset.lance_schema.fields()) >= full_max:
        return []
    for fragment in dataset.get_fragments():
        if any(full_max in f.fields for f in fragment.metadata.data_files()):
            return [fragment.fragment_id]
    raise ValueError(f"No fragment holds max_field_id {full_max}")
