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

def test_get_import_data_aggregated(sample_json):
    parser = Parser(sample_json)
    data = parser.get_import_data()
    assert len(data) == 2
    # Find Track A
    track_a = next(d for d in data if d[0][2] == "Track A")
    assert track_a[1] == {"count": 2, "latest_uts": 2000}
    # Find Track B
    track_b = next(d for d in data if d[0][2] == "Track B")
    assert track_b[1] == {"count": 1, "latest_uts": 3000}

def test_get_import_data_with_filters(sample_json):
    parser = Parser(sample_json)
    
    # Range that only includes one occurrence of Track A
    data = parser.get_import_data(since_ts=1500, until_ts=2500)
    assert len(data) == 1
    assert data[0][1] == {"count": 1, "latest_uts": 2000}

def test_iter_scrobbles_missing_uts(tmp_path):
    data = {"track": [{"artist": {"#text": "Artist A"}, "name": "Track A"}]} # No date/uts
    json_path = tmp_path / "no_uts.json"
    json_path.write_text(json.dumps(data))
    
    parser = Parser(json_path)
    scrobbles = list(parser.iter_scrobbles())
    assert len(scrobbles) == 0

def test_iter_scrobbles_empty_file(tmp_path):
    json_path = tmp_path / "empty.json"
    json_path.write_text(json.dumps([]))
    
    parser = Parser(json_path)
    scrobbles = list(parser.iter_scrobbles())
    assert len(scrobbles) == 0
