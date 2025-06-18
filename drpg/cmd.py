from __future__ import annotations

import argparse
import configparser
import json # For config saving/loading
import logging
import os # Needed for default db path logic
import os.path
import platform
import re
import signal
import sys
from os import environ
from pathlib import Path, PurePath
from traceback import format_exception
from typing import TYPE_CHECKING

import httpx

import drpg
from drpg.config import Config
from drpg.sync import DrpgSync  # Import DrpgSync here

if TYPE_CHECKING:  # pragma: no cover
    from types import FrameType, TracebackType

    CliArgs = list[str]

__all__ = ["run"]


def run() -> None:
    signal.signal(signal.SIGINT, _handle_signal)
    sys.excepthook = _excepthook
    config = load_config()
    _setup_logger(config.log_level)

    # Ensure the database directory exists
    config.library_path.parent.mkdir(parents=True, exist_ok=True)
    config.db_path.parent.mkdir(parents=True, exist_ok=True)

    logging.info(
        "Launching sync with: \n"
        f"  - Dry Run: {config.dry_run}\n"
        f"  - Library: {config.library_path}\n"
        f"  - DB: {config.db_path}\n"
        f"  - Use Checksums: {config.use_checksums}\n"
        f"  - Validate: {config.validate}\n"
        f"  - Threads: {config.threads}\n\n"
    )
    DrpgSync(config).sync()

    save_config(vars(config))

def _default_library_dir() -> Path:
    os_name = platform.system()
    if os_name == "Linux":
        xdg_config = Path(environ.get("XDG_CONFIG_HOME", "~/.config")).expanduser()
        try:
            with open(xdg_config / "user-dirs.dirs") as f:
                raw_config = "[xdg]\n" + f.read().replace('"', "")
            config = configparser.ConfigParser()
            config.read_string(raw_config)
            raw_dir = config["xdg"]["xdg_documents_dir"]
            dir = Path(os.path.expandvars(raw_dir))
        except (FileNotFoundError, KeyError):
            raw_dir = "$HOME/Documents"
            dir = Path(os.path.expandvars(raw_dir))
    elif os_name == "Windows":
        dir = Path.home()
    else:
        dir = Path.cwd()
    return dir / "DriveThruRPG"

def _default_config_dir() -> Path:
    os_name = platform.system()
    if os_name == "Linux":
        dir = Path(environ.get("XDG_CONFIG_HOME", "~/.config")).expanduser()
    elif os_name == "Windows":
        dir = Path.home()
    else:
        dir = Path.cwd()
    return dir / "drpg"


def _default_db_path() -> Path:
    return _default_config_dir() / "library.db"

# Configuration file path
CONFIG_FILE = _default_config_dir() / "drpg_config.json"

# Default configuration
DEFAULT_CONFIG = {
    "library_path": _default_library_dir(),
    "token": "", # Default to empty
    "use_checksums": False,
    "use_cached_products": False,
    "validate": True,
    "compatibility_mode": False,
    "omit_publisher": False,
    "threads": 5,
    "log_level": "INFO",
    "dry_run": False,
    "db_path": str(_default_db_path()), # Add default db_path
}

# --- Configuration Loading/Saving ---
def load_config() -> Config:
    """Loads configuration from JSON file, returning defaults if not found or invalid."""
    # Ensure all keys are present, add defaults if missing
    config = DEFAULT_CONFIG.copy()
    if CONFIG_FILE.exists():
        try:
            with open(CONFIG_FILE, "r") as f:
                data = json.load(f)
                # Ensure loaded keys exist in defaults before updating
                valid_data = {k: v for k, v in data.items() if k in config}
                config.update(valid_data) # Overwrite defaults with loaded data
                # Ensure specific types
                config["threads"] = int(config.get("threads", 5))
                # Ensure paths are strings for JSON, but we'll convert later
                config["library_path"] = str(config.get("library_path", DEFAULT_CONFIG["library_path"]))
                config["db_path"] = str(config.get("db_path", DEFAULT_CONFIG["db_path"]))
        except (json.JSONDecodeError, IOError, ValueError, TypeError) as e: # Added TypeError
            # Use basic print for early errors before logging might be set up
            print(f"Error loading config file {CONFIG_FILE}: {e}. Using defaults.", file=sys.stderr)
            config = DEFAULT_CONFIG.copy()
    # Parse any command line args
    return _parse_cli(config)

def save_config(config_data: dict) -> None:
    """Saves configuration to JSON file, ensuring paths are strings."""
    try:
        # Create a copy to modify for saving
        save_data = config_data.copy()
        save_data["dry_run"] = False
        # Ensure paths are strings
        if isinstance(save_data.get("library_path"), Path):
             save_data["library_path"] = str(save_data["library_path"])
        if isinstance(save_data.get("db_path"), Path):
             save_data["db_path"] = str(save_data["db_path"])

        # Ensure parent directory for config file exists
        CONFIG_FILE.parent.mkdir(parents=True, exist_ok=True)

        with open(CONFIG_FILE, "w") as f:
            json.dump(save_data, f, indent=4)
        logging.info(f"Configuration saved to {CONFIG_FILE}")
    except IOError as e:
        logging.error(f"Error saving config file {CONFIG_FILE}: {e}")

def _parse_cli(config: dict, args: CliArgs | None = None) -> Config:
    parser = argparse.ArgumentParser(
        prog="drpg",
        description=f"""
            Download and keep up to date your purchases from DriveThruRPG.
            Version {drpg.__version__}.
        """,
        epilog="""
            Instead of parameters you can use environment variables. Prefix
            an option with DRPG_, capitalize it and replace '-' with '_'.
            For instance '--use-checksums' becomes 'DRPG_USE_CHECKSUMS=true'.
        """,
    )
    parser.add_argument(
        "--token",
        "-t",
        required="DRPG_TOKEN" not in environ,
        help="Required. Your DriveThruRPG API token",
        default=environ.get("DRPG_TOKEN", config["token"]),
    )
    parser.add_argument(
        "--library-path",
        "-p",
        default=environ.get("DRPG_LIBRARY_PATH", config["library_path"]),
        type=Path,
        help=f"Path to your downloads. Defaults to {config["library_path"]}",
    )
    parser.add_argument(
        "--use-checksums",
        "-c",
        action="store_true",
        default=environ.get("DRPG_USE_CHECKSUMS", str(config["use_checksums"])).lower() == "true",
        help="Decide if a file needs to be downloaded based on checksums. Slower but more precise",
    )
    parser.add_argument(
        "--use-cached-products",
        action="store_true",
        default=environ.get("DRPG_USE_CACHED_PRODUCTS", str(config["use_cached_products"])).lower() == "false",
        help="Skip updating products list from the API",
    )
    parser.add_argument(
        "--validate",
        "-v",
        action="store_true",
        default=environ.get("DRPG_VALIDATE", str(config["validate"])).lower() == "true",
        help="Validate downloads by calculating checksums",
    )
    parser.add_argument(
        "--log-level",
        default=environ.get("DRPG_LOG_LEVEL", config["log_level"]),
        choices=[logging.getLevelName(i) for i in range(10, 60, 10)],
        help="How verbose the output should be. Defaults to 'INFO'",
    )
    parser.add_argument(
        "--threads",
        "-x",
        type=int,
        default=int(environ.get("DRPG_THREADS", str(config["threads"]))),
        help="Specify number of threads used to download products",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        default=environ.get("DRPG_DRY_RUN", str(config["dry_run"])).lower() == "true",
        help="Determine what should be downloaded, but do not download it. Defaults to false",
    )
    parser.add_argument(
        "--db-path",
        type=Path,
        default=environ.get("DRPG_DB_PATH", config["db_path"]),
        help=f"Path to the library metadata database. Defaults to {config["db_path"]}",
    )

    compability_group = parser.add_mutually_exclusive_group()

    compability_group.add_argument(
        "--compatibility-mode",
        action="store_true",
        default=environ.get("DRPG_COMPATIBILITY_MODE", str(config["compatibility_mode"])).lower() == "true",
        help="Name files and directories the way that DriveThruRPG's client app does.",
    )
    compability_group.add_argument(
        "--omit-publisher",
        action="store_true",
        default=environ.get("DRPG_OMIT_PUBLISHER", str(config["omit_publisher"])).lower() == "true",
        help="Omit the publisher name in the target path.",
    )

    parsed_args = parser.parse_args(args)
    print(f"parsed args: {parsed_args}")

    cfg = Config()
    cfg.token = parsed_args.token
    cfg.library_path = parsed_args.library_path
    cfg.use_checksums = parsed_args.use_checksums
    cfg.use_cached_products = parsed_args.use_cached_products
    cfg.validate = parsed_args.validate
    cfg.log_level = parsed_args.log_level
    cfg.dry_run = parsed_args.dry_run
    cfg.compatibility_mode = parsed_args.compatibility_mode
    cfg.omit_publisher = parsed_args.omit_publisher
    cfg.threads = parsed_args.threads
    cfg.db_path = parsed_args.db_path

    return cfg


def _setup_logger(level_name: str) -> None:
    level = getattr(logging, level_name.upper(), None)
    if not isinstance(level, int):
        level = logging.INFO
    if level == logging.DEBUG:
        format = "%(name)-8s - %(asctime)s - %(message)s"
    else:
        format = "%(message)s"

    log_filename = _default_config_dir() / "drpg.log" # Log to home dir

    handler = logging.StreamHandler(sys.stdout)
    handler.setLevel(level)
    handler.addFilter(application_key_filter)
    logging.basicConfig(
        format=format,
        handlers=[
            handler,
            logging.FileHandler(log_filename, mode='a'),
        ],
        level=level,
    )
    _set_httpx_log_level(level)


def _set_httpx_log_level(level: int):
    if level == logging.DEBUG:
        httpx_log_level = logging.DEBUG
        httpx_deps_log_level = logging.INFO
    else:
        httpx_log_level = logging.WARNING
        httpx_deps_log_level = logging.WARNING

    logger = logging.getLogger("httpx")
    logger.setLevel(httpx_log_level)

    for name in ("httpcore", "hpack"):
        logger = logging.getLogger(name)
        logger.setLevel(httpx_deps_log_level)


def _handle_signal(sig: int, frame: FrameType | None) -> None:
    logging.getLogger("drpg").info("Stopping...")
    sys.exit(0)


def _excepthook(
    exc_type: type[BaseException], exc: BaseException, tb: TracebackType | None
) -> None:
    logger = logging.getLogger("drpg")
    logger.error("Unexpected error occurred, stopping!")
    logger.info("".join(format_exception(exc_type, exc, tb)))


_APPLICATION_KEY_RE = re.compile(r"(applicationKey=)(.{10,40})")


def application_key_filter(record: logging.LogRecord):
    try:
        method, url, *other = record.args  # type: ignore
        if (
            record.name == "httpx"
            and isinstance(url, httpx.URL)
            and url.params.get("applicationKey")
        ):
            url = re.sub(_APPLICATION_KEY_RE, r"\1******", str(url))
            record.args = (method, url) + tuple(other)
    except Exception:
        pass
    return True
