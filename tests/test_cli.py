import pytest
import sqlite3
import json
import logging
from click.testing import CliRunner
from navidrome_lastfm_history_import.cli import main, parse_iso_date

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

@pytest.fixture
def setup_data(tmp_path):
    # Mock DB
    db_path = tmp_path / "navidrome.db"
    conn = sqlite3.connect(db_path)
    conn.execute("CREATE TABLE user (id TEXT PRIMARY KEY, user_name TEXT, name TEXT)")
    conn.execute("""
        CREATE TABLE media_file (
            id TEXT PRIMARY KEY, 
            artist_id TEXT,
            album_id TEXT,
            artist TEXT, 
            album TEXT, 
            title TEXT
        )
    """)
    conn.execute("""
        CREATE TABLE annotation (
            user_id TEXT, 
            item_id TEXT, 
            item_type TEXT, 
            play_count INTEGER, 
            play_date DATETIME,
            UNIQUE(user_id, item_id, item_type)
        )
    """)
    conn.execute("INSERT INTO user VALUES ('u1', 'admin', 'Admin')")
    conn.execute("INSERT INTO media_file VALUES ('m1', 'art1', 'alb1', 'Artist A', 'Album A', 'Track A')")
    conn.commit()
    conn.close()

    # Mock JSON
    json_data = [
        {
            "track": [
                {
                    "artist": {"#text": "Artist A"},
                    "album": {"#text": "Album A"},
                    "name": "Track A",
                    "date": {"uts": "1000"}
                }
            ]
        }
    ]
    json_path = tmp_path / "lastfm.json"
    json_path.write_text(json.dumps(json_data))
    
    return db_path, json_path

def test_cli_basic_run(setup_data, caplog):
    db_path, json_path = setup_data
    runner = CliRunner()
    with caplog.at_level(logging.INFO):
        result = runner.invoke(main, [str(db_path), str(json_path), '--user', 'admin'])
    assert result.exit_code == 0
    assert "Found 1 unique tracks matching filters" in caplog.text
    assert "Matched 1 unique tracks" in caplog.text

    # Verify DB update
    conn = sqlite3.connect(db_path)
    row = conn.execute("SELECT play_count FROM annotation WHERE item_id = 'm1'").fetchone()
    assert row[0] == 1
    conn.close()

def test_cli_dry_run(setup_data, caplog):
    db_path, json_path = setup_data
    runner = CliRunner()
    with caplog.at_level(logging.INFO):
        result = runner.invoke(main, [str(db_path), str(json_path), '--user', 'admin', '--dry-run'])
    assert result.exit_code == 0
    assert "Dry run completed" in caplog.text

    # Verify DB NOT updated
    conn = sqlite3.connect(db_path)
    row = conn.execute("SELECT count(*) FROM annotation").fetchone()
    assert row[0] == 0
    conn.close()

def test_cli_user_not_found(setup_data, caplog):
    db_path, json_path = setup_data
    runner = CliRunner()
    with caplog.at_level(logging.ERROR):
        result = runner.invoke(main, [str(db_path), str(json_path), '--user', 'nonexistent'])
    assert result.exit_code == 0
    assert "User 'nonexistent' not found" in caplog.text

def test_cli_date_filters(setup_data, caplog):
    db_path, json_path = setup_data
    runner = CliRunner()
    # Filter out everything
    with caplog.at_level(logging.INFO):
        result = runner.invoke(main, [str(db_path), str(json_path), '--user', 'admin', '--since', '2023-01-01'])
    assert result.exit_code == 0
    assert "No scrobbles to import" in caplog.text

def test_cli_before_existing(setup_data, caplog):
    db_path, json_path = setup_data
    
    # Add an existing play at ts=500
    conn = sqlite3.connect(db_path)
    conn.execute("INSERT INTO annotation VALUES ('u1', 'm1', 'media_file', 1, '1970-01-01 00:08:20')") # ts 500
    conn.commit()
    conn.close()
    
    runner = CliRunner()
    # Scrobble in JSON is at ts=1000. So --before-existing should filter it out.
    with caplog.at_level(logging.INFO):
        result = runner.invoke(main, [str(db_path), str(json_path), '--user', 'admin', '--before-existing'])
    
    assert result.exit_code == 0
    assert "Filtering scrobbles before existing play date" in caplog.text
    assert "No scrobbles to import" in caplog.text
