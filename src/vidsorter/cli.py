import re
import shutil
import sys
from pathlib import Path
import yt_dlp
from datetime import datetime
from typing import Optional, List, Dict, Any

import structlog
import colorama
from rich.console import Console
from rich.logging import RichHandler
from rich.progress import (
    Progress,
    SpinnerColumn,
    TextColumn,
    BarColumn,
    TimeElapsedColumn,
)
from rich.table import Table
from rich.panel import Panel
from rich import print as rprint

colorama.init(autoreset=True)
console = Console()

SUPPORTED_FORMATS = {
    "video": [
        ".mp4",
        ".mkv",
        ".avi",
        ".mov",
        ".wmv",
        ".flv",
        ".webm",
        ".m4v",
        ".3gp",
        ".ogv",
    ],
    "audio": [".mp3", ".m4a", ".wav", ".flac", ".ogg", ".aac", ".opus", ".wma"],
}

ALL_FORMATS = set(SUPPORTED_FORMATS["video"] + SUPPORTED_FORMATS["audio"])


def setup_structured_logging():
    """Set up structured logging with rich formatting and colors"""

    structlog.configure(
        processors=[
            structlog.stdlib.filter_by_level,
            structlog.stdlib.add_logger_name,
            structlog.stdlib.add_log_level,
            structlog.stdlib.PositionalArgumentsFormatter(),
            structlog.processors.TimeStamper(fmt="iso"),
            structlog.processors.StackInfoRenderer(),
            structlog.processors.format_exc_info,
            structlog.processors.UnicodeDecoder(),
            structlog.processors.JSONRenderer(),
        ],
        context_class=dict,
        logger_factory=structlog.stdlib.LoggerFactory(),
        wrapper_class=structlog.stdlib.BoundLogger,
        cache_logger_on_first_use=True,
    )

    import logging

    logging.basicConfig(
        level=logging.INFO,
        format="%(message)s",
        datefmt="[%X]",
        handlers=[RichHandler(console=console, rich_tracebacks=True)],
    )

    logger = structlog.get_logger("media_organizer")

    return logger


class MediaOrganizerStats:
    """Class to track and display processing statistics"""

    def __init__(self):
        self.total_files = 0
        self.processed_files = 0
        self.failed_files = 0
        self.video_files = 0
        self.audio_files = 0
        self.channels_created = set()
        self.start_time = datetime.now()
        self.errors = []

    def add_error(self, filename: str, error_type: str, details: str):
        """Add an error to the error log"""
        self.errors.append(
            {
                "filename": filename,
                "error_type": error_type,
                "details": details,
                "timestamp": datetime.now().isoformat(),
            }
        )

    def add_channel(self, channel_name: str):
        """Add a channel to the set of created channels"""
        self.channels_created.add(channel_name)

    def increment_processed(self):
        """Increment processed file count"""
        self.processed_files += 1

    def increment_failed(self):
        """Increment failed file count"""
        self.failed_files += 1

    def get_summary(self) -> Dict[str, Any]:
        """Get summary statistics"""
        end_time = datetime.now()
        duration = end_time - self.start_time

        return {
            "total_files": self.total_files,
            "processed_files": self.processed_files,
            "failed_files": self.failed_files,
            "video_files": self.video_files,
            "audio_files": self.audio_files,
            "channels_created": len(self.channels_created),
            "duration_seconds": duration.total_seconds(),
            "success_rate": (self.processed_files / self.total_files * 100)
            if self.total_files > 0
            else 0,
        }


def extract_video_id(filename: str) -> Optional[str]:
    """
    Extract video ID from filename in format: <title> [<video ID>].<ext>
    Returns the video ID if found, None otherwise
    """
    extensions_pattern = "|".join(re.escape(ext[1:]) for ext in ALL_FORMATS)
    pattern = rf".*\[([a-zA-Z0-9_-]{{11}})\]\.({extensions_pattern})$"
    match = re.search(pattern, filename, re.IGNORECASE)
    return match.group(1) if match else None


def get_youtube_metadata(video_id: str, logger) -> Optional[str]:
    """
    Fetch YouTube metadata for a given video ID
    Returns channel name or None if failed
    """
    ydl_opts = {"quiet": True, "no_warnings": True, "extract_flat": False}

    try:
        with yt_dlp.YoutubeDL(ydl_opts) as ydl:
            info = ydl.extract_info(
                f"https://www.youtube.com/watch?v={video_id}", download=False
            )

            if not isinstance(info, dict):
                logger.error(
                    "Invalid metadata format received",
                    video_id=video_id,
                    info_type=type(info).__name__,
                    info_content=str(info)[:100] if info else None,
                )
                return None

            channel_name = info.get("uploader") or info.get("channel")

            if channel_name:
                logger.info(
                    "Successfully fetched metadata",
                    video_id=video_id,
                    channel_name=channel_name,
                    video_title=info.get("title", "Unknown"),
                    upload_date=info.get("upload_date", "Unknown"),
                )
            return channel_name

    except Exception as e:
        logger.error(
            "Failed to fetch YouTube metadata",
            video_id=video_id,
            error=str(e),
            error_type=type(e).__name__,
        )
        return None


def sanitize_directory_name(name: str) -> str:
    """
    Sanitize channel name to be a valid directory name
    """
    sanitized = re.sub(r'[<>:"/\\|?*]', "_", name)
    sanitized = sanitized.strip(". ")
    return sanitized if sanitized else "Unknown_Channel"


def create_channel_directory(channel_name: str, logger) -> Optional[Path]:
    """
    Create a directory for the channel if it doesn't exist
    """
    sanitized_name = sanitize_directory_name(channel_name)
    channel_dir = Path(sanitized_name)

    try:
        created = not channel_dir.exists()
        channel_dir.mkdir(exist_ok=True)

        if created:
            logger.info(
                "Created new channel directory",
                channel_name=channel_name,
                directory_path=str(channel_dir),
                sanitized_name=sanitized_name,
            )
        else:
            logger.debug(
                "Using existing channel directory",
                channel_name=channel_name,
                directory_path=str(channel_dir),
            )

        return channel_dir

    except Exception as e:
        logger.error(
            "Failed to create channel directory",
            channel_name=channel_name,
            sanitized_name=sanitized_name,
            error=str(e),
            error_type=type(e).__name__,
        )
        return None


def move_file_to_channel_directory(file_path: Path, channel_dir: Path, logger) -> bool:
    """
    Move the media file to the channel directory
    """
    try:
        destination = channel_dir / file_path.name
        counter = 1
        original_destination = destination

        while destination.exists():
            stem = original_destination.stem
            suffix = original_destination.suffix
            destination = channel_dir / f"{stem}_{counter}{suffix}"
            counter += 1

        if counter > 1:
            logger.warning(
                "File renamed to avoid conflict",
                original_name=file_path.name,
                new_name=destination.name,
                conflict_count=counter - 1,
            )

        shutil.move(str(file_path), str(destination))

        logger.info(
            "Successfully moved file",
            source_file=file_path.name,
            destination=str(destination),
            channel_directory=str(channel_dir),
            file_size_mb=round(destination.stat().st_size / (1024 * 1024), 2),
        )

        return True

    except Exception as e:
        logger.error(
            "Failed to move file",
            source_file=file_path.name,
            destination_dir=str(channel_dir),
            error=str(e),
            error_type=type(e).__name__,
        )
        return False


def get_file_type(file_path: Path) -> str:
    """
    Determine if the file is audio or video based on its extension
    """
    extension = file_path.suffix.lower()
    if extension in SUPPORTED_FORMATS["video"]:
        return "video"
    elif extension in SUPPORTED_FORMATS["audio"]:
        return "audio"
    else:
        return "unknown"


def find_media_files(logger) -> List[Path]:
    """
    Find all supported media files in the current directory
    """
    current_dir = Path(".")
    media_files = []

    with console.status("[bold blue]Scanning for media files..."):
        for extension in ALL_FORMATS:
            pattern = f"*{extension}"
            files = list(current_dir.glob(pattern))
            files.extend(list(current_dir.glob(f"*{extension.upper()}")))
            media_files.extend(files)

    media_files = list(set(media_files))

    logger.info(
        "Media file scan completed",
        total_files_found=len(media_files),
        scan_directory=str(current_dir.absolute()),
    )

    return media_files


def display_startup_info(logger):
    """
    Display startup information with supported formats
    """
    table = Table(
        title="🎬 Media Organizer - Supported Formats",
        show_header=True,
        header_style="bold magenta",
    )
    table.add_column("Type", style="cyan", no_wrap=True)
    table.add_column("Extensions", style="green")

    table.add_row("Video", ", ".join(SUPPORTED_FORMATS["video"]))
    table.add_row("Audio", ", ".join(SUPPORTED_FORMATS["audio"]))

    console.print(table)
    console.print()

    logger.info(
        "Media organizer started",
        supported_video_formats=len(SUPPORTED_FORMATS["video"]),
        supported_audio_formats=len(SUPPORTED_FORMATS["audio"]),
        total_supported_formats=len(ALL_FORMATS),
    )


def display_final_summary(stats: MediaOrganizerStats, logger):
    """
    Display a comprehensive final summary
    """
    summary = stats.get_summary()

    summary_text = f"""
📊 [bold]Processing Summary[/bold]
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

📁 Files Processed: {summary["processed_files"]}/{summary["total_files"]}
✅ Success Rate: {summary["success_rate"]:.1f}%
🎬 Video Files: {summary["video_files"]}
🎵 Audio Files: {summary["audio_files"]}
📂 Channels Created: {summary["channels_created"]}
⏱️  Duration: {summary["duration_seconds"]:.2f}s

"""

    if summary["success_rate"] >= 90:
        panel_style = "green"
        emoji = "🎉"
    elif summary["success_rate"] >= 70:
        panel_style = "yellow"
        emoji = "⚠️"
    else:
        panel_style = "red"
        emoji = "❌"

    console.print(
        Panel(summary_text, title=f"{emoji} Final Results", style=panel_style)
    )

    logger.info("Processing completed", **summary)

    if stats.errors:
        error_table = Table(
            title="❌ Processing Errors", show_header=True, header_style="bold red"
        )
        error_table.add_column("File", style="cyan")
        error_table.add_column("Error Type", style="red")
        error_table.add_column("Details", style="white")

        for error in stats.errors[:10]:
            error_table.add_row(
                error["filename"],
                error["error_type"],
                error["details"][:50] + "..."
                if len(error["details"]) > 50
                else error["details"],
            )

        if len(stats.errors) > 10:
            error_table.add_row(
                "...", "...", f"... and {len(stats.errors) - 10} more errors"
            )

        console.print(error_table)


def process_media_files():
    """
    Main function to process all supported media files in the current directory
    """
    logger = setup_structured_logging()
    stats = MediaOrganizerStats()

    display_startup_info(logger)

    try:
        media_files = find_media_files(logger)

        if not media_files:
            rprint(
                "[yellow]ℹ️  No supported media files found in the current directory.[/yellow]"
            )
            return

        stats.total_files = len(media_files)
        for file_path in media_files:
            file_type = get_file_type(file_path)
            if file_type == "video":
                stats.video_files += 1
            elif file_type == "audio":
                stats.audio_files += 1

        with Progress(
            SpinnerColumn(),
            TextColumn("[progress.description]{task.description}"),
            BarColumn(),
            TextColumn("[progress.percentage]{task.percentage:>3.0f}%"),
            TimeElapsedColumn(),
            console=console,
        ) as progress:
            task = progress.add_task(
                "Processing media files...", total=len(media_files)
            )

            for media_file in media_files:
                file_type = get_file_type(media_file)

                progress.update(
                    task,
                    description=f"Processing {file_type}: {media_file.name[:30]}...",
                )

                logger.info(
                    "Starting file processing",
                    filename=media_file.name,
                    file_type=file_type,
                    file_size_mb=round(media_file.stat().st_size / (1024 * 1024), 2),
                )

                video_id = extract_video_id(media_file.name)
                if not video_id:
                    error_msg = "Could not extract video ID from filename"
                    logger.warning(
                        "Video ID extraction failed", filename=media_file.name
                    )
                    stats.add_error(media_file.name, "ID_EXTRACTION_FAILED", error_msg)
                    stats.increment_failed()
                    progress.advance(task)
                    continue

                channel_name = get_youtube_metadata(video_id, logger)
                if not channel_name:
                    error_msg = "Could not fetch channel metadata"
                    stats.add_error(media_file.name, "METADATA_FETCH_FAILED", error_msg)
                    stats.increment_failed()
                    progress.advance(task)
                    continue

                channel_dir = create_channel_directory(channel_name, logger)
                if not channel_dir:
                    error_msg = "Could not create channel directory"
                    stats.add_error(
                        media_file.name, "DIRECTORY_CREATION_FAILED", error_msg
                    )
                    stats.increment_failed()
                    progress.advance(task)
                    continue

                stats.add_channel(channel_name)

                if move_file_to_channel_directory(media_file, channel_dir, logger):
                    stats.increment_processed()
                else:
                    error_msg = "Could not move file to channel directory"
                    stats.add_error(media_file.name, "FILE_MOVE_FAILED", error_msg)
                    stats.increment_failed()

                progress.advance(task)

    except KeyboardInterrupt:
        logger.warning("Processing interrupted by user")
        rprint("[yellow]⚠️  Processing interrupted by user[/yellow]")
    except Exception as e:
        logger.error(
            "Unexpected error during processing",
            error=str(e),
            error_type=type(e).__name__,
        )
        rprint(f"[red]❌ Unexpected error: {e}[/red]")
    finally:
        display_final_summary(stats, logger)


def main():
    try:
        process_media_files()
    except Exception as e:
        console.print(f"[red]💥 Fatal error: {e}[/red]")
        sys.exit(1)


if __name__ == "__main__":
    main()
