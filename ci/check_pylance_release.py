#!/usr/bin/env python3
"""Determine whether a newer Pylance tag exists and expose results for CI."""

from __future__ import annotations

import argparse
import json
import os
import re
import subprocess
import sys
from collections.abc import Iterable, Sequence
from dataclasses import dataclass
from pathlib import Path

try:
    import tomllib
except ModuleNotFoundError:
    import tomli as tomllib  # type: ignore

LANCE_REPO = "lance-format/lance"

SEMVER_RE = re.compile(
    r"^\s*(?P<major>0|[1-9]\d*)\.(?P<minor>0|[1-9]\d*)\.(?P<patch>0|[1-9]\d*)"
    r"(?:-(?P<prerelease>[0-9A-Za-z.-]+))?"
    r"(?:\+[0-9A-Za-z.-]+)?\s*$"
)

PYPI_PRERELEASE_RE = re.compile(r"^(?P<base>\d+\.\d+\.\d+)(?P<pre>a|b|rc)(?P<num>\d+)$")

PYLANCE_DEP_RE = re.compile(r"^pylance[><=!~]+(.+)$")


def pypi_to_semver(version: str) -> str:
    """Convert PyPI version format to semver format.

    Examples:
        2.0.0b8  -> 2.0.0-beta.8
        2.0.0a1  -> 2.0.0-alpha.1
        2.0.0rc1 -> 2.0.0-rc.1
        2.0.0    -> 2.0.0
    """
    match = PYPI_PRERELEASE_RE.match(version.strip())
    if not match:
        return version
    base = match.group("base")
    pre = match.group("pre")
    num = match.group("num")
    pre_map = {"a": "alpha", "b": "beta", "rc": "rc"}
    return f"{base}-{pre_map[pre]}.{num}"


@dataclass(frozen=True)
class SemVer:
    major: int
    minor: int
    patch: int
    prerelease: tuple[int | str, ...]

    def __lt__(self, other: SemVer) -> bool:
        if (self.major, self.minor, self.patch) != (
            other.major,
            other.minor,
            other.patch,
        ):
            return (self.major, self.minor, self.patch) < (
                other.major,
                other.minor,
                other.patch,
            )
        if self.prerelease == other.prerelease:
            return False
        if not self.prerelease:
            return False  # release > anything else
        if not other.prerelease:
            return True
        for left, right in zip(self.prerelease, other.prerelease, strict=False):
            if left == right:
                continue
            if isinstance(left, int) and isinstance(right, int):
                return left < right
            if isinstance(left, int):
                return True
            if isinstance(right, int):
                return False
            return str(left) < str(right)
        return len(self.prerelease) < len(other.prerelease)

    def __eq__(self, other: object) -> bool:
        if not isinstance(other, SemVer):
            return NotImplemented
        return (
            self.major == other.major
            and self.minor == other.minor
            and self.patch == other.patch
            and self.prerelease == other.prerelease
        )


def parse_semver(raw: str) -> SemVer:
    match = SEMVER_RE.match(raw)
    if not match:
        raise ValueError(f"Unsupported version format: {raw}")
    prerelease = match.group("prerelease")
    parts: tuple[int | str, ...] = ()
    if prerelease:
        parsed: list[int | str] = []
        for piece in prerelease.split("."):
            if piece.isdigit():
                parsed.append(int(piece))
            else:
                parsed.append(piece)
        parts = tuple(parsed)
    return SemVer(
        major=int(match.group("major")),
        minor=int(match.group("minor")),
        patch=int(match.group("patch")),
        prerelease=parts,
    )


@dataclass
class TagInfo:
    tag: str  # e.g. v1.0.0-beta.2
    version: str  # e.g. 1.0.0-beta.2
    semver: SemVer


def run_command(cmd: Sequence[str]) -> str:
    result = subprocess.run(cmd, capture_output=True, text=True, check=False)
    if result.returncode != 0:
        raise RuntimeError(
            f"Command {' '.join(cmd)} failed with {result.returncode}: {result.stderr.strip()}"
        )
    return result.stdout.strip()


def fetch_remote_tags() -> list[TagInfo]:
    output = run_command(
        [
            "gh",
            "api",
            "-X",
            "GET",
            f"repos/{LANCE_REPO}/git/refs/tags",
            "--paginate",
            "--jq",
            ".[].ref",
        ]
    )
    tags: list[TagInfo] = []
    for line in output.splitlines():
        ref = line.strip()
        if not ref.startswith("refs/tags/v"):
            continue
        tag = ref.split("refs/tags/")[-1]
        version = tag.lstrip("v")
        try:
            tags.append(TagInfo(tag=tag, version=version, semver=parse_semver(version)))
        except ValueError:
            continue
    if not tags:
        raise RuntimeError("No Pylance tags could be parsed from GitHub API output")
    return tags


def read_current_version(repo_root: Path) -> str:
    pyproject_path = repo_root / "pyproject.toml"
    with pyproject_path.open("rb") as fh:
        data = tomllib.load(fh)
    try:
        deps = data["project"]["dependencies"]
    except KeyError as exc:
        raise RuntimeError(
            "Failed to locate project.dependencies in pyproject.toml"
        ) from exc

    for dep in deps:
        match = PYLANCE_DEP_RE.match(dep)
        if match:
            return match.group(1).strip()

    raise RuntimeError("pylance dependency not found in project.dependencies")


def determine_latest_tag(tags: Iterable[TagInfo]) -> TagInfo:
    return max(tags, key=lambda tag: tag.semver)


def write_outputs(args: argparse.Namespace, payload: dict) -> None:
    target = getattr(args, "github_output", None)
    if not target:
        return
    with open(target, "a", encoding="utf-8") as handle:
        for key, value in payload.items():
            handle.write(f"{key}={value}\n")


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--repo-root",
        default=Path(__file__).resolve().parents[1],
        type=Path,
        help="Path to the lance-ray repository root",
    )
    parser.add_argument(
        "--github-output",
        default=os.environ.get("GITHUB_OUTPUT"),
        help="Optional file path for writing GitHub Action outputs",
    )
    args = parser.parse_args(argv)

    repo_root = Path(args.repo_root)
    current_pypi_version = read_current_version(repo_root)
    current_version = pypi_to_semver(current_pypi_version)
    current_semver = parse_semver(current_version)

    tags = fetch_remote_tags()
    latest = determine_latest_tag(tags)
    needs_update = latest.semver > current_semver

    payload = {
        "current_version": current_version,
        "current_tag": f"v{current_version}",
        "latest_version": latest.version,
        "latest_tag": latest.tag,
        "needs_update": "true" if needs_update else "false",
    }

    print(json.dumps(payload))
    write_outputs(args, payload)
    return 0


if __name__ == "__main__":
    sys.exit(main())
