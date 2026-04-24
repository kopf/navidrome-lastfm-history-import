import click
from pathlib import Path
import logging
from datetime import datetime, timezone
from rich.console import Console
from rich.logging import RichHandler
from rich.progress import track
from typing import Optional

from .db import Database
from .parser import Parser

console = Console()

def parse_iso_date(date_str: Optional[str]) -> Optional[int]:
    if not date_str:
        return None
    try:
        s = date_str.replace('Z', '+00:00')
        dt = datetime.fromisoformat(s)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return int(dt.timestamp())
    except ValueError as e:
        raise click.BadParameter(f"Invalid ISO date format: {e}")

@click.command()
@click.argument('db_path', type=click.Path(exists=True, path_type=Path))
@click.argument('json_path', type=click.Path(exists=True, path_type=Path))
@click.option('--user', '-u', required=True, help='Navidrome username to import plays for.')
@click.option('--fuzzy', is_flag=True, help='Enable slightly fuzzy matching for tracks.')
@click.option('--since', help='Only import scrobbles after this ISO date (e.g. 2023-01-01).')
@click.option('--until', help='Only import scrobbles before this ISO date.')
@click.option('--before-existing', is_flag=True, help='Only import scrobbles that occurred before the first existing play in Navidrome.')
@click.option('--dry-run', is_flag=True, help='Show what would be done without modifying the database.')
@click.option('--verbose', '-v', is_flag=True, help='Enable verbose logging.')
def main(db_path: Path, json_path: Path, user: str, fuzzy: bool, since: str, until: str, before_existing: bool, dry_run: bool, verbose: bool):
    """Import Last.fm history JSON into Navidrome SQLite database."""
    
    log_level = logging.DEBUG if verbose else logging.INFO
    logging.basicConfig(
        level=log_level,
        format="%(message)s",
        datefmt="[%X]",
        handlers=[RichHandler(console=console, rich_tracebacks=True, markup=True)]
    )
    logger = logging.getLogger("navidrome_import")

    since_ts = parse_iso_date(since)
    until_ts = parse_iso_date(until)

    db = Database(db_path)
    try:
        user_id = db.get_user_id(user)
        if not user_id:
            logger.error(f"User '{user}' not found in Navidrome database.")
            return

        if before_existing:
            earliest_ts = db.get_earliest_play_timestamp(user_id)
            if earliest_ts:
                logger.info(f"Filtering scrobbles before existing play date: {datetime.fromtimestamp(earliest_ts, tz=timezone.utc)}")
                if until_ts is None or earliest_ts < until_ts:
                    until_ts = earliest_ts
            else:
                logger.warning(f"No existing plays found for user '{user}'. --before-existing filter ignored.")

        logger.info(f"Loading and aggregating data from {json_path}...")
        parser = Parser(json_path)
        import_data = parser.get_import_data(since_ts=since_ts, until_ts=until_ts)
        logger.info(f"Found {len(import_data)} unique tracks matching filters.")

        if not import_data:
            logger.info("No scrobbles to import.")
            return

        matched_count = 0
        skipped_count = 0
        
        for (artist, album, title), stats in track(
            import_data, 
            description="Importing tracks...", 
            console=console
        ):
            track_info = db.find_track(artist, album, title, fuzzy=fuzzy)
            
            if track_info:
                media_file_id = track_info["id"]
                logger.info(f"Updating: [bold cyan]{artist}[/bold cyan] - [bold white]{title}[/bold white] ([bold yellow]{media_file_id}[/bold yellow]) (+{stats['count']} plays)")
                db.update_plays(user_id, track_info, stats['count'], stats['latest_uts'], dry_run=dry_run)
                matched_count += 1
            else:
                logger.debug(f"Could not match: {artist} - {album} - {title}")
                skipped_count += 1

        if not dry_run:
            logger.info("Committing changes to database...")
            db.commit()
            logger.info("Changes committed.")
        else:
            logger.info("Dry run completed. No changes were made.")
        
        logger.info(f"Summary: Matched {matched_count} unique tracks, Skipped {skipped_count} tracks.")
        
    finally:
        db.close()

if __name__ == '__main__':
    main()
