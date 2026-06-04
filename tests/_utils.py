"""Shared helpers for tests."""

import inspect


def missing_fragment_write_options(*options: str) -> tuple[str, ...]:
    from lance.fragment import write_fragments

    params = inspect.signature(write_fragments).parameters
    return tuple(sorted(set(options).difference(params)))


def fragment_write_options_skip_reason(*options: str) -> str:
    missing = missing_fragment_write_options(*options)
    return (
        "Installed pylance does not expose the missing fragment write "
        "option(s) on lance.fragment.write_fragments: "
        f"{', '.join(missing)}"
    )
