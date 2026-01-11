__all__ = ("test_version_comparison",)


def test_version_comparison() -> None:

    # start-example
    from sqlspec.migrations.version import parse_version

    v1 = parse_version("0001")
    v2 = parse_version("20251018120000")

    # Sequential < Timestamp (by design)
    assert v1 < v2

    # Same type comparisons work naturally
    assert parse_version("0001") < parse_version("0002")
    assert parse_version("20251018120000") < parse_version("20251019120000")
    # end-example
