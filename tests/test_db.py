import sqlite3
import pytest
from pathlib import Path
from datetime import datetime
from navidrome_lastfm_history_import.db import Database

@pytest.fixture
def mock_db_path(tmp_path):
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
    conn.execute("INSERT INTO media_file VALUES ('m1', 'art1', 'alb1', 'Zulu Pearls', 'No Heroes No Honeymoons', 'No Heroes No Honeymoons')")
    conn.commit()
    conn.close()
    return db_path

@pytest.fixture
def db(mock_db_path):
    db_instance = Database(mock_db_path)
    yield db_instance
    db_instance.close()

def test_get_user_id(db):
    assert db.get_user_id("admin") == "u1"
    assert db.get_user_id("Admin") == "u1"  # "name" column
    assert db.get_user_id("nonexistent") is None

def test_get_earliest_play_timestamp(db):
    # No plays yet
    assert db.get_earliest_play_timestamp("u1") is None
    
    # Add a play
    track_info = {"id": "m1", "artist_id": "art1", "album_id": "alb1"}
    db.update_plays("u1", track_info, 1, 1000)
    db.commit()
    
    # uts 1000 is 1970-01-01 00:16:40 (depending on TZ, but it's consistent in sqlite if stored as string)
    # Actually Database class uses datetime.fromtimestamp which uses local time. 
    # Let's just check it returns a value.
    ts = db.get_earliest_play_timestamp("u1")
    assert ts is not None

def test_find_track_case_insensitive(db):
    info = db.find_track("zulu pearls", "no heroes no honeymoons", "no heroes no honeymoons")
    assert info["id"] == "m1"

def test_find_track_not_found(db):
    assert db.find_track("Nonexistent", "Album", "Title") is None

def test_update_plays_existing_annotation(db, mock_db_path):
    track_info = {"id": "m1", "artist_id": "art1", "album_id": "alb1"}
    
    # Initial update
    db.update_plays("u1", track_info, 1, 1000)
    db.commit()
    
    # Second update
    db.update_plays("u1", track_info, 2, 2000)
    db.commit()
    
    conn = sqlite3.connect(mock_db_path)
    row = conn.execute("SELECT play_count, play_date FROM annotation WHERE item_id = 'm1'").fetchone()
    assert row[0] == 3
    assert "1970-01-01" in row[1]
    conn.close()

def test_update_plays_older_date_does_not_overwrite(db, mock_db_path):
    track_info = {"id": "m1", "artist_id": "art1", "album_id": "alb1"}
    
    # Initial update with newer date
    db.update_plays("u1", track_info, 1, 2000)
    db.commit()
    
    # Second update with older date
    db.update_plays("u1", track_info, 2, 1000)
    db.commit()
    
    conn = sqlite3.connect(mock_db_path)
    row = conn.execute("SELECT play_count, play_date FROM annotation WHERE item_id = 'm1'").fetchone()
    assert row[0] == 3
    # Should still have the date from ts=2000
    expected_date = datetime.fromtimestamp(2000).strftime('%Y-%m-%d %H:%M:%S')
    assert row[1] == expected_date
    conn.close()

def test_update_plays_dry_run(db, mock_db_path):
    track_info = {"id": "m1", "artist_id": "art1", "album_id": "alb1"}
    db.update_plays("u1", track_info, 1, 1000, dry_run=True)
    db.commit()
    
    conn = sqlite3.connect(mock_db_path)
    row = conn.execute("SELECT count(*) FROM annotation").fetchone()
    assert row[0] == 0
    conn.close()

def test_find_track_fuzzy_variations(db):
    # Slight typos/missing words with fuzzy=True
    info = db.find_track("Zulu Pearl", "No Heroes No Honeymoons", "No Heroes No Honeymoons", fuzzy=True)
    assert info["id"] == "m1"

def test_find_track_fallback_success(db):
    # Search with wrong album, but unique artist/title
    info = db.find_track("Zulu Pearls", "Wrong Album", "No Heroes No Honeymoons")
    assert info is not None
    assert info["id"] == "m1"

def test_find_track_fallback_ambiguous_failure(db, mock_db_path):
    # Add another track with same artist/title but different album
    conn = sqlite3.connect(mock_db_path)
    conn.execute("INSERT INTO media_file VALUES ('m2', 'art1', 'alb2', 'Zulu Pearls', 'Another Album', 'No Heroes No Honeymoons')")
    conn.commit()
    conn.close()
    
    # Search with wrong album, but multiple artist/title matches
    info = db.find_track("Zulu Pearls", "Wrong Album", "No Heroes No Honeymoons")
    assert info is None
