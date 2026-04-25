import sqlite3
import logging
from pathlib import Path
from typing import Optional, List, Dict, Tuple
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
            try:
                dt = datetime.strptime(row[0], '%Y-%m-%d %H:%M:%S')
                return int(dt.timestamp())
            except ValueError:
                return None
        return None

    def _normalize(self, text: str) -> str:
        if not text:
            return ""
        # Normalize '&' to 'and'
        text = text.lower().replace("&", " and ")
        # Replace punctuation with spaces
        text = re.sub(r"[^\w\s]", " ", text)
        # Collapse multiple spaces and strip
        return " ".join(text.split())

    def find_track(self, artist: str, album: str, title: str, fuzzy: bool = False) -> Optional[Dict[str, str]]:
        """
        Finds a track and returns its ID, artist_id, and album_id.
        """
        # Exact match
        query = "SELECT id, artist_id, album_id FROM media_file WHERE artist = ? AND album = ? AND title = ?"
        row = self.conn.execute(query, (artist, album, title)).fetchone()
        if row:
            return dict(row)

        # Case-insensitive
        query = "SELECT id, artist_id, album_id FROM media_file WHERE artist LIKE ? AND album LIKE ? AND title LIKE ?"
        row = self.conn.execute(query, (artist, album, title)).fetchone()
        if row:
            return dict(row)

        # Fallback: Artist + Title match (ignoring album)
        query = "SELECT id, artist_id, album_id FROM media_file WHERE artist LIKE ? AND title LIKE ?"
        rows = self.conn.execute(query, (artist, title)).fetchall()
        if len(rows) == 1:
            return dict(rows[0])

        if fuzzy:
            # Create a broad search pattern for candidates
            # Replace ' & ', ' and ', and spaces with '%' to find potential matches
            broad_artist = artist.lower().replace(" & ", " ").replace(" and ", " ")
            broad_artist = re.sub(r"[^\w\s]", " ", broad_artist)
            search_pattern = "%" + "%".join(broad_artist.split()) + "%"

            query = "SELECT id, artist_id, album_id, artist, album, title FROM media_file WHERE artist LIKE ?"
            candidates = self.conn.execute(query, (search_pattern,)).fetchall()
            
            best_score = 0
            best_match = None
            norm_target = f"{self._normalize(artist)} {self._normalize(album)} {self._normalize(title)}"
            
            for cand in candidates:
                norm_cand = f"{self._normalize(cand['artist'])} {self._normalize(cand['album'])} {self._normalize(cand['title'])}"
                score = fuzz.token_set_ratio(norm_target, norm_cand)
                if score > 90 and score > best_score:
                    best_score = score
                    best_match = {"id": cand["id"], "artist_id": cand["artist_id"], "album_id": cand["album_id"]}
            return best_match

        return None

    def update_item_plays(self, user_id: str, item_id: str, item_type: str, count: int, last_play_date: str, dry_run: bool = False):
        """
        Helper to update or insert annotation for a specific item type.
        """
        if not item_id or dry_run:
            return

        query = "SELECT play_count, play_date FROM annotation WHERE user_id = ? AND item_id = ? AND item_type = ?"
        row = self.conn.execute(query, (user_id, item_id, item_type)).fetchone()

        if row:
            new_count = row["play_count"] + count
            current_date_str = row["play_date"]
            if not current_date_str or last_play_date > current_date_str:
                update_query = """
                    UPDATE annotation 
                    SET play_count = ?, play_date = ? 
                    WHERE user_id = ? AND item_id = ? AND item_type = ?
                """
                self.conn.execute(update_query, (new_count, last_play_date, user_id, item_id, item_type))
            else:
                update_query = """
                    UPDATE annotation 
                    SET play_count = ? 
                    WHERE user_id = ? AND item_id = ? AND item_type = ?
                """
                self.conn.execute(update_query, (new_count, user_id, item_id, item_type))
        else:
            insert_query = """
                INSERT INTO annotation (user_id, item_id, item_type, play_count, play_date)
                VALUES (?, ?, ?, ?, ?)
            """
            self.conn.execute(insert_query, (user_id, item_id, item_type, count, last_play_date))

    def update_plays(self, user_id: str, track_info: Dict[str, str], count: int, latest_timestamp: int, dry_run: bool = False):
        """
        Updates play_count and play_date for track, album, and artist.
        """
        last_play_date = datetime.fromtimestamp(latest_timestamp).strftime('%Y-%m-%d %H:%M:%S')
        
        # Update Track
        self.update_item_plays(user_id, track_info["id"], "media_file", count, last_play_date, dry_run)
        
        # Update Album
        if track_info.get("album_id"):
            self.update_item_plays(user_id, track_info["album_id"], "album", count, last_play_date, dry_run)
            
        # Update Artist
        if track_info.get("artist_id"):
            self.update_item_plays(user_id, track_info["artist_id"], "artist", count, last_play_date, dry_run)
