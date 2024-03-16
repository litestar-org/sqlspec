from sqlspec.loader import remove_multiline_comments
from sqlspec.patterns import UNCOMMENT, VAR_REF


def test_var_pattern_is_quote_aware() -> None:
    sql = r"""
          select foo_id,
                 bar_id,
                 to_char(created_at, 'YYYY-MM-DD"T"HH24:MI:SSOF')
            from foo
            join bars using(bar_id)
            join baz using(baz_id)
           where created_at < :created_at_mark
             and foo_mark > :foo_mark
        order by created_at desc, source_name asc;
    """
    group_dicts = [m.groupdict() for m in VAR_REF.finditer(sql)]
    assert len(group_dicts) == 3

    expected = [
        {
            "dquote": None,
            "lead": None,
            "squote": "'YYYY-MM-DD\"T\"HH24:MI:SSOF'",
            "var_name": None,
        },
        {
            "dquote": None,
            "lead": " ",
            "squote": None,
            "var_name": "created_at_mark",
        },
        {"dquote": None, "lead": " ", "squote": None, "var_name": "foo_mark"},
    ]
    assert group_dicts == expected


def test_var_pattern_does_not_require_semicolon_trail() -> None:
    """Make sure keywords ending queries are recognized even without
    semi-colons.
    """
    sql = r"""
        select a,
               b,
               c
          FROM foo
         WHERE a = :a"""

    group_dicts = [m.groupdict() for m in VAR_REF.finditer(sql)]
    assert len(group_dicts) == 1

    expected = {"dquote": None, "lead": " ", "squote": None, "var_name": "a"}
    assert group_dicts[0] == expected


def test_var_pattern_handles_empty_sql_string_literals() -> None:
    """Make sure SQL '' are treated correctly and don't cause a substitution to be skipped."""
    sql = r"""
        select blah
          from foo
         where lower(regexp_replace(blah,'\\W','','g')) = lower(regexp_replace(:blah,'\\W','','g'));"""

    group_dicts = [m.groupdict() for m in VAR_REF.finditer(sql)]

    expected_single_quote_match = {
        "dquote": None,
        "lead": None,
        "squote": "''",
        "var_name": None,
    }
    assert group_dicts[1] == expected_single_quote_match

    expected_var_match = {
        "dquote": None,
        "lead": "(",
        "squote": None,
        "var_name": "blah",
    }
    assert group_dicts[3] == expected_var_match


# must remove only OK comments
COMMENTED = """
KO
-- KO
/* OK */
'/* KO */'
"/* KO */"
' /* KO
   */'
" /* KO
   */"
/*
 * OK
 */
-- /* KO
-- */
/* OK
  -- OK
  ' OK ' "OK "
 */
KO
/* OK */ -- KO 'KO'
-- KO */
"""


def test_comments() -> None:
    n = 0
    for m in UNCOMMENT.finditer(COMMENTED):  # sourcery skip: no-loop-in-tests
        n += 1
        matches = m.groupdict()
        s, d, c, m = matches["squote"], matches["dquote"], matches["oneline"], matches["multiline"]
        assert s or d or c or m
        if m:
            assert "KO" not in m
        if s:
            assert "OK" not in s
        if d:
            assert "OK" not in d
        if c:
            assert "OK" not in c
    assert n == 13


COMMENT_UNCOMMENT = [
    ("", ""),
    ("hello", "hello"),
    ("world!\n", "world!\n"),
    ("/**/", ""),
    ("x/*\n*/y\n", "xy\n"),
    ("-- /* */\n", "-- /* */\n"),
    ("-- /* */", "-- /* */"),
    ("'/* */'", "'/* */'"),
    ("--\n/* */X\n", "--\nX\n"),
]


def test_uncomment():
    n = 0
    for c, u in COMMENT_UNCOMMENT:  # sourcery skip: no-loop-in-tests
        n += 1
        assert remove_multiline_comments(c) == u
    assert n == len(COMMENT_UNCOMMENT)
