import json
import pytest
from pathlib import Path
from navidrome_lastfm_history_import.parser import Parser

@pytest.fixture
def sample_json(tmp_path):
    data = [
        {
            "track": [
                {
                    "artist": {"#text": "Artist A"},
                    "album": {"#text": "Album A"},
                    "name": "Track A",
                    "date": {"uts": "1000"}
                },
                {
                    "artist": {"#text": "Artist A"},
                    "album": {"#text": "Album A"},
                    "name": "Track A",
                    "date": {"uts": "2000"}
                },
                {
                    "artist": {"#text": "Artist B"},
                    "album": {"#text": "Album B"},
                    "name": "Track B",
                    "date": {"uts": "3000"}
                }
            ]
        }
    ]
    json_path = tmp_path / "test.json"
    json_path.write_text(json.dumps(data))
    return json_path

def test_get_import_data_with_filters(sample_json):
    parser = Parser(sample_json)
    
    # Test 'since' filter
    data = parser.get_import_data(since_ts=1500)
    assert len(data) == 2
    assert data[0][1]['latest_uts'] == 2000
    assert data[1][1]['latest_uts'] == 3000

    # Test 'until' filter
    data = parser.get_import_data(until_ts=2500)
    assert len(data) == 2
    assert data[0][1]['latest_uts'] == 1000
    assert data[1][1]['latest_uts'] == 2000

    # Test range
    data = parser.get_import_data(since_ts=1500, until_ts=2500)
    assert len(data) == 1
    assert data[0][1]['latest_uts'] == 2000

def test_get_import_data_aggregated_with_filters(sample_json):
    parser = Parser(sample_json)
    
    # Aggregate only track A (uts 1000 and 2000)
    data = parser.get_import_data(aggregate=True, until_ts=2500)
    assert len(data) == 1
    assert data[0][0] == ("Artist A", "Album A", "Track A")
    assert data[0][1] == {"count": 2, "latest_uts": 2000}
