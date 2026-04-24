import pytest
from navidrome_lastfm_history_import.cli import parse_iso_date

def test_parse_iso_date():
    # Simple date
    assert parse_iso_date("2023-01-01") == 1672531200
    # Date with time
    assert parse_iso_date("2023-01-01T12:00:00") == 1672574400
    # ISO format with Z
    assert parse_iso_date("2023-01-01T00:00:00Z") == 1672531200
    # Invalid date
    with pytest.raises(Exception):
        parse_iso_date("not-a-date")
    # None handling
    assert parse_iso_date(None) is None
