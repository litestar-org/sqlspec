from pathlib import Path

try:
    import tomllib
except ImportError:  # pragma: no cover
    import tomli as tomllib


PROJECT_ROOT = Path(__file__).parents[2]


def test_framework_optional_dependencies_are_defined() -> None:
    pyproject = tomllib.loads((PROJECT_ROOT / "pyproject.toml").read_text())

    optional_dependencies = pyproject["project"]["optional-dependencies"]

    assert optional_dependencies["fastapi"] == ["fastapi"]
    assert optional_dependencies["flask"] == ["flask"]
    assert optional_dependencies["litestar"] == ["litestar"]
    assert optional_dependencies["sanic"] == ["sanic"]
    assert optional_dependencies["starlette"] == ["starlette"]


def test_sanic_testing_dependency_is_defined() -> None:
    pyproject = tomllib.loads((PROJECT_ROOT / "pyproject.toml").read_text())

    test_dependencies = pyproject["dependency-groups"]["test"]

    assert "sanic-testing" in test_dependencies
