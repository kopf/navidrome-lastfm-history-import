import json
from pathlib import Path
from typing import Dict, List, Any, Tuple, Generator, Optional
from collections import defaultdict

class Parser:
    def __init__(self, json_path: Path):
        self.json_path = json_path

    def iter_scrobbles(self, since_ts: Optional[int] = None, until_ts: Optional[int] = None) -> Generator[Dict[str, Any], None, None]:
        """
        Yields scrobbles from the JSON file, optionally filtered by timestamp.
        """
        with open(self.json_path, 'r') as f:
            data = json.load(f)
            
        if isinstance(data, list):
            pages = data
        elif isinstance(data, dict):
            pages = [data]
        else:
            pages = []

        for page in pages:
            tracks = page.get('track', [])
            if isinstance(tracks, dict):
                tracks = [tracks]
            for track in tracks:
                uts_str = track.get('date', {}).get('uts')
                if not uts_str:
                    continue
                uts = int(uts_str)
                
                if since_ts and uts < since_ts:
                    continue
                if until_ts and uts > until_ts:
                    continue
                    
                yield track

    def get_import_data(
        self, 
        since_ts: Optional[int] = None, 
        until_ts: Optional[int] = None
    ) -> List[Tuple[Tuple[str, str, str], Dict[str, Any]]]:
        """
        Returns a list of aggregated (key, stats) tuples, optionally filtered by timestamp.
        """
        aggregated = defaultdict(lambda: {'count': 0, 'latest_uts': 0})
        for track_data in self.iter_scrobbles(since_ts=since_ts, until_ts=until_ts):
            artist = track_data.get('artist', {}).get('#text', '')
            album = track_data.get('album', {}).get('#text', '')
            title = track_data.get('name', '')
            uts = int(track_data.get('date', {}).get('uts', 0))
            
            key = (artist, album, title)
            aggregated[key]['count'] += 1
            if uts > aggregated[key]['latest_uts']:
                aggregated[key]['latest_uts'] = uts
        return list(aggregated.items())
