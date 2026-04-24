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
    conn.execute("INSERT INTO media_file VALUES ('m1', 'Zulu Pearls', 'No Heroes No Honeymoons', 'No Heroes No Honeymoons')")
    conn.execute("INSERT INTO media_file VALUES ('m2', 'The Beatles', 'Abbey Road', 'Come Together')")
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
    assert db.get_user_id("nonexistent") is None

def test_find_track_exact(db):
    track_id = db.find_track("Zulu Pearls", "No Heroes No Honeymoons", "No Heroes No Honeymoons")
    assert track_id == "m1"

def test_find_track_fuzzy_variations(db):
    # Case variation
    assert db.find_track("zulu pearls", "no heroes no honeymoons", "no heroes no honeymoons") == "m1"
    
    # Slight typos/missing words with fuzzy=True
    assert db.find_track("Zulu Pearl", "No Heroes No Honeymoons", "No Heroes No Honeymoons", fuzzy=True) == "m1"
    assert db.find_track("Zulu Pearls", "No Heroes", "No Heroes No Honeymoons", fuzzy=True) == "m1"
    
    # No match even with fuzzy
    assert db.find_track("Radiohead", "Kid A", "Everything", fuzzy=True) is None

def test_get_earliest_play_timestamp(db):
    # Empty case
    assert db.get_earliest_play_timestamp("u1") is None
    
    # Add plays
    db.update_plays("u1", "m1", 1, 2000) # 2000 epoch
    db.update_plays("u1", "m2", 1, 1000) # 1000 epoch
    db.commit()
    
    assert db.get_earliest_play_timestamp("u1") == 1000

def test_update_plays_new(db, mock_db_path):
    db.update_plays("u1", "m1", 5, 1776956273)
    db.commit()
    
    conn = sqlite3.connect(mock_db_path)
    row = conn.execute("SELECT play_count, play_date FROM annotation WHERE item_id = 'm1'").fetchone()
    assert row[0] == 5
    assert "2026-04-23" in row[1]
    conn.close()
