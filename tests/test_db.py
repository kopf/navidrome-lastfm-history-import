import sqlite3
import pytest
from pathlib import Path
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

def test_find_track_returns_ids(db):
    info = db.find_track("Zulu Pearls", "No Heroes No Honeymoons", "No Heroes No Honeymoons")
    assert info["id"] == "m1"
    assert info["artist_id"] == "art1"
    assert info["album_id"] == "alb1"

def test_update_plays_all_types(db, mock_db_path):
    track_info = {"id": "m1", "artist_id": "art1", "album_id": "alb1"}
    db.update_plays("u1", track_info, 5, 1776956273)
    db.commit()
    
    conn = sqlite3.connect(mock_db_path)
    # Check media_file
    row = conn.execute("SELECT play_count FROM annotation WHERE item_id = 'm1' AND item_type = 'media_file'").fetchone()
    assert row[0] == 5
    # Check album
    row = conn.execute("SELECT play_count FROM annotation WHERE item_id = 'alb1' AND item_type = 'album'").fetchone()
    assert row[0] == 5
    # Check artist
    row = conn.execute("SELECT play_count FROM annotation WHERE item_id = 'art1' AND item_type = 'artist'").fetchone()
    assert row[0] == 5
    conn.close()

def test_find_track_fuzzy_variations(db):
    # Slight typos/missing words with fuzzy=True
    info = db.find_track("Zulu Pearl", "No Heroes No Honeymoons", "No Heroes No Honeymoons", fuzzy=True)
    assert info["id"] == "m1"
