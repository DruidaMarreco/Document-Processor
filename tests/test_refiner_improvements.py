"""Tests for improved refiner: date parsing, amount parsing, dedup, whitespace."""
from __future__ import annotations

import pytest

from refiner_module.refiner import _try_parse_date, _try_parse_amount, _normalize


# ---------------------------------------------------------------------------
# Date parsing
# ---------------------------------------------------------------------------

def test_iso_date():
    assert _try_parse_date("2024-01-31") == "2024-01-31"


def test_us_slash_date():
    assert _try_parse_date("01/31/2024") == "2024-01-31"


def test_eu_slash_date():
    assert _try_parse_date("31/01/2024") == "2024-01-31"


def test_dot_notation_date():
    assert _try_parse_date("31.01.2024") == "2024-01-31"


def test_dot_notation_short_year():
    assert _try_parse_date("31.01.24") == "2024-01-31"


def test_long_month_name():
    assert _try_parse_date("January 15, 2024") == "2024-01-15"


def test_day_first_long_month():
    assert _try_parse_date("15 January 2024") == "2024-01-15"


def test_abbreviated_month():
    assert _try_parse_date("15 Jan 2024") == "2024-01-15"


def test_ordinal_date():
    assert _try_parse_date("1st January 2024") == "2024-01-01"


def test_ordinal_date_2nd():
    assert _try_parse_date("2nd Feb 2024") == "2024-02-02"


def test_ordinal_date_3rd():
    assert _try_parse_date("3rd March 2024") == "2024-03-03"


def test_ordinal_date_4th():
    assert _try_parse_date("4th April 2024") == "2024-04-04"


def test_iso_with_time_stripped():
    # Should parse the date part of ISO datetime
    result = _try_parse_date("2024-01-31T12:30:00Z")
    assert result == "2024-01-31"


def test_unparseable_returns_none():
    assert _try_parse_date("not a date") is None


# ---------------------------------------------------------------------------
# Amount parsing
# ---------------------------------------------------------------------------

def test_plain_number():
    assert _try_parse_amount("1234.56") == pytest.approx(1234.56)


def test_dollar_prefix():
    assert _try_parse_amount("$1,234.56") == pytest.approx(1234.56)


def test_euro_prefix():
    assert _try_parse_amount("€500.00") == pytest.approx(500.0)


def test_pound_prefix():
    assert _try_parse_amount("£99.99") == pytest.approx(99.99)


def test_k_suffix():
    assert _try_parse_amount("25k") == pytest.approx(25_000)


def test_k_suffix_with_decimal():
    assert _try_parse_amount("1.5k") == pytest.approx(1500)


def test_m_suffix():
    assert _try_parse_amount("2M") == pytest.approx(2_000_000)


def test_invalid_amount_returns_none():
    assert _try_parse_amount("no amount here") is None


def test_comma_separator():
    assert _try_parse_amount("1,234,567.89") == pytest.approx(1_234_567.89)


# ---------------------------------------------------------------------------
# _normalize function
# ---------------------------------------------------------------------------

def test_normalize_dedup_case_insensitive():
    from refiner_module.refiner import _normalize
    result = _normalize("parties", ["Acme Corp", "acme corp", "ACME CORP"])
    assert len(result) == 1


def test_normalize_removes_empty_strings():
    from refiner_module.refiner import _normalize
    result = _normalize("tags", ["foo", "", "bar", ""])
    assert "" not in result
    assert len(result) == 2


def test_normalize_whitespace_collapse():
    from refiner_module.refiner import _normalize
    result = _normalize("name", "  too   many   spaces  ")
    assert result == "too many spaces"


def test_normalize_date_field():
    from refiner_module.refiner import _normalize
    result = _normalize("due_date", "31.01.2024")
    assert result == "2024-01-31"


def test_normalize_amount_field():
    from refiner_module.refiner import _normalize
    result = _normalize("total_amount", "$1,500.00")
    assert result == pytest.approx(1500.0)


def test_normalize_fee_field():
    from refiner_module.refiner import _normalize
    result = _normalize("processing_fee", "£25.00")
    assert result == pytest.approx(25.0)


def test_normalize_list_of_dates():
    from refiner_module.refiner import _normalize
    result = _normalize("payment_dates", ["01/15/2024", "02/15/2024"])
    assert result == ["2024-01-15", "2024-02-15"]
