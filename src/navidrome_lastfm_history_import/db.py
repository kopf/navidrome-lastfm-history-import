import sqlite3
import logging
from pathlib import Path
from typing import Optional, List, Dict
import re
from datetime import datetime
from rapidfuzz import fuzz

logger = logging.getLogger(__name__)

class Database:
    def __init__(self, db_path: Path):
        self.db_path = db_path
        self.conn = sqlite3.connect(db_path)
        self.conn.row_factory = sqlite3.Row

    def close(self):
        self.conn.close()

    def commit(self):
        self.conn.commit()

    def get_user_id(self, username: str) -> Optional[str]:
        query = "SELECT id FROM user WHERE user_name = ? OR name = ?"
        row = self.conn.execute(query, (username, username)).fetchone()
        return row["id"] if row else None

    def get_earliest_play_timestamp(self, user_id: str) -> Optional[int]:
        query = "SELECT MIN(play_date) FROM annotation WHERE user_id = ? AND item_type = 'media_file'"
        row = self.conn.execute(query, (user_id,)).fetchone()
        if row and row[0]:
            # Convert Navidrome's YYYY-MM-DD HH:MM:SS to timestamp
            dt = datetime.strptime(row[0], '%Y-%m-%d %H:%M:%S')
            return int(dt.timestamp())
        return None

    def _normalize(self, text: str) -> str:
        if not text:
            return ""
        return re.sub(r"[^\w\s]", "", text.lower()).strip()

    def find_track(self, artist: str, album: str, title: str, fuzzy: bool = False) -> Optional[str]:
        # Exact match
        query = "SELECT id FROM media_file WHERE artist = ? AND album = ? AND title = ?"
        row = self.conn.execute(query, (artist, album, title)).fetchone()
        if row:
            return row["id"]

        # Case-insensitive
        query = "SELECT id FROM media_file WHERE artist LIKE ? AND album LIKE ? AND title LIKE ?"
        row = self.conn.execute(query, (artist, album, title)).fetchone()
        if row:
            return row["id"]

        if fuzzy:
            # Slightly fuzzy: match by artist first
            query = "SELECT id, artist, album, title FROM media_file WHERE artist LIKE ?"
            candidates = self.conn.execute(query, (f"%{artist}%",)).fetchall()
            
            best_score = 0
            best_id = None
            norm_target = f"{self._normalize(artist)} {self._normalize(album)} {self._normalize(title)}"
            
            for cand in candidates:
                norm_cand = f"{self._normalize(cand['artist'])} {self._normalize(cand['album'])} {self._normalize(cand['title'])}"
                score = fuzz.token_set_ratio(norm_target, norm_cand)
                if score > 90 and score > best_score:
                    best_score = score
                    best_id = cand["id"]
            return best_id

        return None

    def update_plays(self, user_id: str, media_file_id: str, count: int, latest_timestamp: int, dry_run: bool = False):
        """
        Increments play_count and updates play_date in the annotation table.
        Note: Does NOT commit. Caller must call commit().
        """
        last_play_date = datetime.fromtimestamp(latest_timestamp).strftime('%Y-%m-%d %H:%M:%S')
        
        if dry_run:
            return

        # Check if annotation exists
        query = "SELECT play_count, play_date FROM annotation WHERE user_id = ? AND item_id = ? AND item_type = 'media_file'"
        row = self.conn.execute(query, (user_id, media_file_id)).fetchone()

        if row:
            new_count = row["play_count"] + count
            current_date_str = row["play_date"]
            if not current_date_str or last_play_date > current_date_str:
                update_query = """
                    UPDATE annotation 
                    SET play_count = ?, play_date = ? 
                    WHERE user_id = ? AND item_id = ? AND item_type = 'media_file'
                """
                self.conn.execute(update_query, (new_count, last_play_date, user_id, media_file_id))
            else:
                update_query = """
                    UPDATE annotation 
                    SET play_count = ? 
                    WHERE user_id = ? AND item_id = ? AND item_type = 'media_file'
                """
                self.conn.execute(update_query, (new_count, user_id, media_file_id))
        else:
            insert_query = """
                INSERT INTO annotation (user_id, item_id, item_type, play_count, play_date)
                VALUES (?, ?, 'media_file', ?, ?)
            """
            self.conn.execute(insert_query, (user_id, media_file_id, count, last_play_date))
