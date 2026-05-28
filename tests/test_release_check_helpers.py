import pytest
from ci.check_pylance_release import (
    pypi_to_semver,
    semver_to_pypi,
    update_branch_name,
)


@pytest.mark.parametrize(
    ("version", "expected"),
    [
        ("7.1.0", "7.1.0"),
        ("7.1.0-beta.4", "7.1.0-beta.4"),
        ("7.1.0b4", "7.1.0-beta.4"),
        ("7.1.0-rc.2", "7.1.0-rc.2"),
        ("7.1.0rc2", "7.1.0-rc.2"),
    ],
)
def test_pypi_prerelease_versions_convert_to_semver(
    version: str, expected: str
) -> None:
    assert pypi_to_semver(version) == expected


@pytest.mark.parametrize(
    ("version", "expected"),
    [
        ("7.1.0", "7.1.0"),
        ("7.1.0-beta.4", "7.1.0b4"),
        ("7.1.0-alpha.1", "7.1.0a1"),
        ("7.1.0-rc.2", "7.1.0rc2"),
        ("7.1.0b4", "7.1.0b4"),
    ],
)
def test_semver_prerelease_versions_convert_to_pypi(
    version: str, expected: str
) -> None:
    assert semver_to_pypi(version) == expected


@pytest.mark.parametrize(
    ("dependency_name", "version", "expected"),
    [
        ("pylance", "7.1.0", "codex/update-pylance-7-1-0"),
        ("pylance", "7.1.0-beta.4", "codex/update-pylance-7-1-0-beta-4"),
        ("pylance", "7.1.0-rc.2", "codex/update-pylance-7-1-0-rc-2"),
        ("pylance", "1.0.0+build.1", "codex/update-pylance-1-0-0-build-1"),
        ("pylance", "1.0.0--foo", "codex/update-pylance-1-0-0--foo"),
        ("other-extension", "2.0.0", "codex/update-other-extension-2-0-0"),
    ],
)
def test_dependency_update_branch_name_uses_sanitized_parts(
    dependency_name: str, version: str, expected: str
) -> None:
    assert update_branch_name(dependency_name, version) == expected
