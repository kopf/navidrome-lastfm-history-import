# Navidrome Last.fm History Import

Imports your Last.fm listening history into the Navidrome database. 

## Features

- **Smart Matching**: Matches Last.fm tracks to your Navidrome library using Artist, Album, and Title.
- **Fuzzy Matching**: Optional "slightly fuzzy" matching to handle minor metadata discrepancies (powered by `rapidfuzz`).
- **Smart Filtering**: Automatically filter out scrobbles that occurred after your first play in Navidrome using `--before-existing` to avoid overlapping history, or manually, using `--since` and `--until` ISO dates.

## Installation

```bash
# Clone the repository
git clone https://github.com/yourusername/navidrome-lastfm-history-import.git
cd navidrome-lastfm-history-import

# Install dependencies and the project
uv sync
```

## Usage

Once installed, you can run the tool using `uv run`:

```bash
uv run navidrome-import <path_to_navidrome.db> <path_to_history.json> --user <navidrome_username> [OPTIONS]
```

### Options

*   `-u, --user TEXT`: **(Required)** The Navidrome username to import plays for.
*   `--fuzzy`: Enable slightly fuzzy matching for tracks (handles minor typos/punctuation).
*   `--since TEXT`: Only import scrobbles after this ISO date (e.g., `2023-01-01`).
*   `--until TEXT`: Only import scrobbles before this ISO date.
*   `--before-existing`: Only import scrobbles that occurred *before* the earliest play recorded in your Navidrome database for the given user.
*   `--dry-run`: Show what would be matched and updated without actually changing the database.
*   `-v, --verbose`: Enable detailed debug logging.

## Last.fm JSON Export

You can generate one using the tool [here](https://lastfm.ghan.nl/export/).

## Development

Run tests using `pytest`:

```bash
uv run pytest
```
