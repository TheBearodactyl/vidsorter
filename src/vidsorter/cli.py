import argparse
import re
import shutil
import sys
from pathlib import Path
import yt_dlp
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed

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


def parse_args():
    parser = argparse.ArgumentParser(
        description="Organize YouTube media files into channel directories."
    )
    parser.add_argument(
        "-d",
        "--directory",
        type=Path,
        default=Path("."),
        help="Directory to scan for media files (default: current directory)",
    )
    parser.add_argument(
        "--include-video",
        action="store_true",
        help="Include video files (default: include both if no filter specified)",
    )
    parser.add_argument(
        "--include-audio",
        action="store_true",
        help="Include audio files (default: include both if no filter specified)",
    )
    parser.add_argument(
        "-v",
        "--verbose",
        action="store_true",
        help="Enable verbose logging (DEBUG level)",
    )
    parser.add_argument(
        "-q", "--quiet", action="store_true", help="Suppress non-error output"
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Show planned actions without moving files",
    )
    parser.add_argument(
        "--max-errors",
        type=int,
        default=10,
        help="Maximum number of errors to display in summary",
    )
    parser.add_argument(
        "--skip-metadata",
        action="store_true",
        help="Skip fetching YouTube metadata (all go to 'Unknown_Channel')",
    )
    parser.add_argument(
        "--video-exts",
        type=str,
        help="Comma-separated list of video extensions (e.g. .mp4,.mkv)",
    )
    parser.add_argument(
        "--audio-exts",
        type=str,
        help="Comma-separated list of audio extensions (e.g. .mp3,.wav)",
    )
    parser.add_argument(
        "-w",
        "--workers",
        type=int,
        default=4,
        help="Number of concurrent threads to process files (default: 4)",
    )
    return parser.parse_args()


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

colorama.init(autoreset=True)
console = Console()


def process_single_file(file_path, args, logger, stats, progress, task_id):
    file_type = get_file_type(file_path)
    if file_type == "video":
        stats.video_files += 1
    elif file_type == "audio":
        stats.audio_files += 1

    video_id = extract_video_id(file_path.name)
    if not video_id:
        stats.add_error(file_path.name, "ID_EXTRACTION_FAILED", "No video ID found")
        stats.increment_failed()
        progress.update(task_id, advance=1)
        return

    if args.skip_metadata:
        channel_name = "Unknown_Channel"
    else:
        channel_name = get_youtube_metadata(video_id, logger) or "Unknown_Channel"

    channel_dir = create_channel_directory(channel_name, logger)
    if not channel_dir:
        stats.add_error(file_path.name, "DIR_CREATION_FAILED", "Could not create dir")
        stats.increment_failed()
        progress.update(task_id, advance=1)
        return

    stats.add_channel(channel_name)

    if move_file(file_path, channel_dir, logger, dry_run=args.dry_run):
        stats.increment_processed()
    else:
        stats.increment_failed()

    progress.update(task_id, advance=1)


def process_media_files(args):
    logger = setup_structured_logging(args.verbose, args.quiet)
    stats = MediaOrganizerStats()
    display_startup_info(logger)

    try:
        media_files = find_media_files(
            args.directory, logger, args.include_video, args.include_audio
        )
        if not media_files:
            rprint("[yellow]No supported files found[/yellow]")
            return
        stats.total_files = len(media_files)

        with Progress(
            SpinnerColumn(),
            TextColumn("{task.description}"),
            BarColumn(),
            TextColumn("{task.percentage:>3.0f}%"),
            TimeElapsedColumn(),
            console=console,
        ) as progress:
            task = progress.add_task("Processing...", total=len(media_files))
            with ThreadPoolExecutor(max_workers=args.workers) as executor:
                futures = [
                    executor.submit(
                        process_single_file, f, args, logger, stats, progress, task
                    )
                    for f in media_files
                ]
                for _ in as_completed(futures):
                    pass
    finally:
        display_final_summary(stats, logger, args.max_errors)


def setup_structured_logging(verbose=False, quiet=False):
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

    level = logging.DEBUG if verbose else (logging.ERROR if quiet else logging.INFO)
    logging.basicConfig(
        level=level,
        format="%(message)s",
        datefmt="[%X]",
        handlers=[RichHandler(console=console, rich_tracebacks=True)],
    )
    return structlog.get_logger("media_organizer")


class MediaOrganizerStats:
    def __init__(self):
        self.total_files = 0
        self.processed_files = 0
        self.failed_files = 0
        self.video_files = 0
        self.audio_files = 0
        self.channels_created = set()
        self.start_time = datetime.now()
        self.errors = []

    def add_error(self, filename, error_type, details):
        self.errors.append(
            {
                "filename": filename,
                "error_type": error_type,
                "details": details,
                "timestamp": datetime.now().isoformat(),
            }
        )

    def add_channel(self, channel_name):
        self.channels_created.add(channel_name)

    def increment_processed(self):
        self.processed_files += 1

    def increment_failed(self):
        self.failed_files += 1

    def get_summary(self):
        duration = datetime.now() - self.start_time
        return {
            "total_files": self.total_files,
            "processed_files": self.processed_files,
            "failed_files": self.failed_files,
            "video_files": self.video_files,
            "audio_files": self.audio_files,
            "channels_created": len(self.channels_created),
            "duration_seconds": duration.total_seconds(),
            "success_rate": (self.processed_files / self.total_files * 100)
            if self.total_files
            else 0,
        }


def extract_video_id(filename):
    extensions_pattern = "|".join(re.escape(ext[1:]) for ext in ALL_FORMATS)
    match = re.search(
        rf".*\[([a-zA-Z0-9_-]{{11}})\]\.({extensions_pattern})$",
        filename,
        re.IGNORECASE,
    )
    return match.group(1) if match else None


def get_youtube_metadata(video_id, logger):
    ydl_opts = {"quiet": True, "no_warnings": True, "extract_flat": False}
    try:
        with console.status(f"[bold blue]Fetching metadata for {video_id}..."):
            with yt_dlp.YoutubeDL(ydl_opts) as ydl:
                info = ydl.extract_info(
                    f"https://www.youtube.com/watch?v={video_id}", download=False
                )
                if not isinstance(info, dict):
                    return None
                return info.get("uploader") or info.get("channel")
    except Exception as e:
        logger.error("Metadata fetch failed", video_id=video_id, error=str(e))
        return None


def sanitize_directory_name(name):
    sanitized = re.sub(r'[<>:"/\\|?*]', "_", name).strip(". ")
    return sanitized or "Unknown_Channel"


def create_channel_directory(channel_name, logger):
    sanitized = sanitize_directory_name(channel_name)
    path = Path(sanitized)
    try:
        path.mkdir(exist_ok=True)
        return path
    except Exception as e:
        logger.error("Failed to create directory", name=sanitized, error=str(e))
        return None


def move_file(file_path, channel_dir, logger, dry_run=False):
    try:
        destination = channel_dir / file_path.name
        counter = 1
        while destination.exists():
            destination = channel_dir / f"{file_path.stem}_{counter}{file_path.suffix}"
            counter += 1
        if dry_run:
            logger.info("Dry-run: would move", source=file_path, dest=destination)
            return True
        shutil.move(str(file_path), str(destination))
        return True
    except Exception as e:
        logger.error("Move failed", file=file_path, error=str(e))
        return False


def get_file_type(file_path):
    ext = file_path.suffix.lower()
    if ext in SUPPORTED_FORMATS["video"]:
        return "video"
    elif ext in SUPPORTED_FORMATS["audio"]:
        return "audio"
    return "unknown"


def find_media_files(directory, logger, include_video, include_audio):
    exts = []
    if include_video:
        exts.extend(SUPPORTED_FORMATS["video"])
    if include_audio:
        exts.extend(SUPPORTED_FORMATS["audio"])
    if not include_video and not include_audio:
        exts = list(ALL_FORMATS)

    media_files = []
    with console.status("[bold blue]Scanning..."):
        for ext in exts:
            media_files.extend(directory.glob(f"*{ext}"))
            media_files.extend(directory.glob(f"*{ext.upper()}"))
    media_files = list(set(media_files))
    logger.info("Scan complete", total=len(media_files), directory=str(directory))
    return media_files


def display_startup_info(logger):
    table = Table(title="Supported Formats", header_style="bold magenta")
    table.add_column("Type", style="cyan")
    table.add_column("Extensions", style="green")
    table.add_row("Video", ", ".join(SUPPORTED_FORMATS["video"]))
    table.add_row("Audio", ", ".join(SUPPORTED_FORMATS["audio"]))
    console.print(table)
    logger.info("Startup info displayed")


def display_final_summary(stats, logger, max_errors):
    summary = stats.get_summary()
    summary_text = f"""
📊 Files Processed: {summary["processed_files"]}/{summary["total_files"]}
✅ Success Rate: {summary["success_rate"]:.1f}%
🎬 Video Files: {summary["video_files"]}
🎵 Audio Files: {summary["audio_files"]}
📂 Channels Created: {summary["channels_created"]}
⏱️ Duration: {summary["duration_seconds"]:.2f}s
"""
    console.print(Panel(summary_text, title="Final Results", style="green"))
    logger.info("Summary", **summary)

    if stats.errors:
        table = Table(title="Errors", header_style="bold red")
        table.add_column("File")
        table.add_column("Type")
        table.add_column("Details")
        for error in stats.errors[:max_errors]:
            table.add_row(error["filename"], error["error_type"], error["details"][:50])
        if len(stats.errors) > max_errors:
            table.add_row(
                "...", "...", f"... and {len(stats.errors) - max_errors} more"
            )
        console.print(table)


def main():
    args = parse_args()
    if args.video_exts:
        SUPPORTED_FORMATS["video"] = [ext.strip() for ext in args.video_exts.split(",")]
    if args.audio_exts:
        SUPPORTED_FORMATS["audio"] = [ext.strip() for ext in args.audio_exts.split(",")]
    ALL_FORMATS.clear()
    ALL_FORMATS.update(SUPPORTED_FORMATS["video"] + SUPPORTED_FORMATS["audio"])
    try:
        process_media_files(args)
    except Exception as e:
        console.print(f"[red]Fatal error: {e}[/red]")
        sys.exit(1)


if __name__ == "__main__":
    main()
