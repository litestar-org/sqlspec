from pathlib import Path

import pytest
from _pytest.monkeypatch import MonkeyPatch

from sqlspec.base import SyncDatabaseConfig
from sqlspec.utils.module_loader import import_string, module_to_os_path


def test_import_string() -> None:
    cls = import_string("sqlspec.base.SyncDatabaseConfig")
    assert type(cls) is type(SyncDatabaseConfig)

    with pytest.raises(ImportError):
        _ = import_string("GenericAlembicConfigNew")
        _ = import_string("sqlspec.base.GenericAlembicConfigNew")
        _ = import_string("imaginary_module_that_doesnt_exist.Config")  # a random nonexistent class


def test_module_path(tmp_path: Path, monkeypatch: MonkeyPatch) -> None:
    the_path = module_to_os_path("sqlspec.base")
    assert the_path.exists()

    tmp_path.joinpath("simple_module.py").write_text("x = 'foo'")
    monkeypatch.syspath_prepend(tmp_path)  # pyright: ignore[reportUnknownMemberType]
    os_path = module_to_os_path("simple_module")
    assert os_path == Path(tmp_path)
    with pytest.raises(
        (
            ImportError,
            TypeError,
        )
    ):
        _ = module_to_os_path("sqlspec.base.GenericDatabaseConfig")
        _ = module_to_os_path("sqlspec.base.GenericDatabaseConfig.extra.module")


def test_import_non_existing_attribute_raises() -> None:
    with pytest.raises(ImportError):
        import_string("sqlspec.base.GenericDatabaseConfig.extra.module")


def test_import_string_cached(tmp_path: Path, monkeypatch: MonkeyPatch) -> None:
    tmp_path.joinpath("testmodule.py").write_text("x = 'foo'")
    monkeypatch.chdir(tmp_path)
    monkeypatch.syspath_prepend(tmp_path)  # pyright: ignore[reportUnknownMemberType]
    assert import_string("testmodule.x") == "foo"
