from __future__ import annotations

import functools
import html
import logging
import re
import sqlite3
from datetime import datetime, timedelta, timezone as dt_timezone
from hashlib import md5
from multiprocessing.pool import ThreadPool
from time import timezone # Keep this for the timedelta calculation, maybe rename later
from typing import TYPE_CHECKING, NamedTuple

import httpx

from drpg.api import DrpgApi
from drpg.custom_types import PrepareDownloadUrlResponse # Import this type

if TYPE_CHECKING:  # pragma: no cover
    from pathlib import Path
    from typing import Any, Callable

    from drpg.config import Config
    from drpg.custom_types import DownloadItem, Product

    NoneCallable = Callable[..., None]
    Decorator = Callable[[NoneCallable], NoneCallable]

logger = logging.getLogger("drpg")

# Define a structure for DB query results for type hinting
class DbFileInfo(NamedTuple):
    api_last_modified: str | None
    api_checksum: str | None
    local_path: str | None
    local_last_synced: str | None
    local_checksum: str | None


def suppress_errors(*errors: type[Exception]) -> Decorator:
    """Silence but log provided errors."""

    def decorator(func: NoneCallable) -> NoneCallable:
        @functools.wraps(func)
        def wrapper(*args: Any, **kwargs: Any) -> None:
            try:
                return func(*args, **kwargs)
            except errors as e:
                logger.exception(e)

        return wrapper

    return decorator


class DrpgSync:
    """High level DriveThruRPG client that syncs products from a customer's library using a DB cache."""

    _DB_SCHEMA = """
    CREATE TABLE IF NOT EXISTS products (
        product_id INTEGER PRIMARY KEY,
        name TEXT NOT NULL,
        publisher_name TEXT,
        last_api_check TIMESTAMP
    );

    CREATE TABLE IF NOT EXISTS files (
        product_id INTEGER NOT NULL,
        item_id INTEGER NOT NULL,
        filename TEXT NOT NULL,
        api_last_modified TEXT, -- Store as ISO string
        api_checksum TEXT,
        local_path TEXT NOT NULL,
        local_last_synced TEXT, -- Store as ISO string
        local_checksum TEXT,
        PRIMARY KEY (product_id, item_id),
        FOREIGN KEY (product_id) REFERENCES products(product_id) ON DELETE CASCADE
    );

    CREATE INDEX IF NOT EXISTS idx_files_local_path ON files (local_path);
    """

    def __init__(self, config: Config) -> None:
        self._config = config
        self._api = DrpgApi(config.token)
        self._db_conn = sqlite3.connect(config.db_path, isolation_level=None) # Autocommit mode
        self._db_conn.row_factory = sqlite3.Row # Access columns by name
        self._db_conn.execute("PRAGMA foreign_keys = ON;")
        self._setup_db()
        self._touched_items = set() # Keep track of items seen in current API sync

    def _setup_db(self) -> None:
        """Create database tables if they don't exist."""
        with self._db_conn:
            self._db_conn.executescript(self._DB_SCHEMA)
        logger.debug("Database schema initialized at %s", self._config.db_path)

    def sync(self) -> None:
        """Download all new, updated and not yet synced items to a sync directory using DB cache."""
        logger.info("Authenticating")
        self._api.token()
        logger.info("Fetching products list from API")
        self._touched_items.clear() # Reset for this sync run

        # Prepare arguments for parallel processing
        process_item_args = []
        try:
            for product in self._api.customer_products():
                # Update product info in DB (or insert if new)
                self._update_product_in_db(product)
                for item in product["files"]:
                    item_key = (product["orderProductId"], item["index"])
                    self._touched_items.add(item_key) # Mark as seen in API
                    process_item_args.append((product, item))
        except Exception as e:
            logger.error("Failed to fetch products from API: %s", e, exc_info=True)
            self._close_db()
            return

        logger.info("Checking %d items against local cache/filesystem", len(process_item_args))
        # Use ThreadPool for downloading, but decisions are made sequentially before this
        items_to_download = []
        for product, item in process_item_args:
            if self._need_download_db(product, item):
                items_to_download.append((product, item))

        logger.info("Found %d items requiring download/update.", len(items_to_download))
        if items_to_download:
            with ThreadPool(self._config.threads) as pool:
                pool.starmap(self._process_item_db, items_to_download)

        # Cleanup DB - remove items not seen in the API response
        self._cleanup_db()

        self._close_db()
        logger.info("Sync finished!")

    def _update_product_in_db(self, product: Product) -> None:
        """Insert or update product information in the database."""
        now = datetime.now(dt_timezone.utc).isoformat()
        publisher_name = product.get("publisher", {}).get("name")
        with self._db_conn:
            self._db_conn.execute(
                """
                INSERT INTO products (product_id, name, publisher_name, last_api_check)
                VALUES (?, ?, ?, ?)
                ON CONFLICT(product_id) DO UPDATE SET
                    name = excluded.name,
                    publisher_name = excluded.publisher_name,
                    last_api_check = excluded.last_api_check;
                """,
                (product["orderProductId"], product["name"], publisher_name, now),
            )

    def _get_db_file_info(self, product_id: int, item_id: int) -> DbFileInfo | None:
        """Fetch file metadata from the database."""
        cursor = self._db_conn.execute(
            """
            SELECT api_last_modified, api_checksum, local_path, local_last_synced, local_checksum
            FROM files
            WHERE product_id = ? AND item_id = ?
            """,
            (product_id, item_id),
        )
        row = cursor.fetchone()
        return DbFileInfo(*row) if row else None

    def _need_download_db(self, product: Product, item: DownloadItem) -> bool:
        """Check DB cache and filesystem to determine if download is needed."""
        product_id = product["orderProductId"]
        item_id = item["index"]
        expected_path = self._file_path(product, item)
        db_info = self._get_db_file_info(product_id, item_id)

        if not db_info:
            logger.debug(
                "Needs download: %s - %s: No record in DB cache",
                product["name"], item["filename"]
            )
            return True

        # Check if path changed due to config
        if str(expected_path) != db_info.local_path:
             logger.debug(
                "Needs download: %s - %s: Local path changed ('%s' vs '%s')",
                product["name"], item["filename"], db_info.local_path, expected_path
            )
             return True

        # Check API modification time against DB cache
        api_mod_time_str = product["fileLastModified"]
        if api_mod_time_str != db_info.api_last_modified:
            logger.debug(
                "Needs download: %s - %s: API modification time changed ('%s' vs '%s')",
                 product["name"], item["filename"], db_info.api_last_modified, api_mod_time_str
            )
            return True

        # Check checksum if enabled
        if self._config.use_checksums:
            api_checksum = _newest_checksum(item)
            if api_checksum != db_info.api_checksum:
                 logger.debug(
                    "Needs download: %s - %s: API checksum changed ('%s' vs '%s')",
                    product["name"], item["filename"], db_info.api_checksum, api_checksum
                )
                 return True
            # Optional: If checksums match, double-check filesystem if file exists?
            # Could add a check here against path.exists() and local checksum if paranoid.
            # For performance, we trust the DB if checksums match.

        # Fallback: Check if file actually exists at the cached path (maybe deleted manually)
        # This adds a stat call but prevents errors if DB is out of sync with reality.
        if not expected_path.exists():
             logger.debug(
                "Needs download: %s - %s: File missing at cached path '%s'",
                product["name"], item["filename"], expected_path
            )
             return True

        logger.info("Up to date (cached): %s - %s", product["name"], item["filename"])
        return False

    @suppress_errors(httpx.HTTPError, PermissionError, sqlite3.Error, DrpgApi.PrepareDownloadUrlException)
    def _process_item_db(self, product: Product, item: DownloadItem) -> None:
        """Download an item and update the database cache."""
        path = self._file_path(product, item)
        product_id = product["orderProductId"]
        item_id = item["index"]

        if self._config.dry_run:
            logger.info("DRY RUN - would have downloaded file: %s", path)
            # In dry run, maybe update DB as if downloaded? Or skip DB update?
            # Let's skip DB update for dry run to keep it simple.
            return

        logger.info("Processing: %s - %s", product["name"], item["filename"])

        # 1. Get Download URL
        try:
            url_data = self._api.prepare_download_url(product_id, item_id)
        except self._api.PrepareDownloadUrlException as e:
            logger.warning(
                "Could not get download URL for %s - %s: %s",
                product["name"], item["filename"], e
            )
            return # Don't update DB if we can't even get the URL

        # 2. Download File
        try:
            file_response = httpx.get(
                url_data["url"],
                timeout=60.0, # Slightly longer timeout for downloads
                follow_redirects=True,
                headers={
                    "Accept-Encoding": "gzip, deflate, br",
                    "User-Agent": "Mozilla/5.0",
                    "Accept": "*/*",
                },
            )
            file_response.raise_for_status() # Raise exception for bad status codes
            file_content = file_response.content
        except httpx.HTTPStatusError as e:
             logger.error(
                "HTTP error downloading %s - %s: %s",
                product["name"], item["filename"], e
            )
             return
        except httpx.RequestError as e:
            logger.error(
                "Network error downloading %s - %s: %s",
                product["name"], item["filename"], e
            )
            return

        # 3. Validate Checksum (if enabled)
        local_checksum = None
        api_checksum = _newest_checksum(item)
        if self._config.validate and api_checksum:
            local_checksum = md5(file_content).hexdigest()
            if local_checksum != api_checksum:
                logger.error(
                    "ERROR: Invalid checksum for %s - %s, skipping saving file (API: %s != Local: %s)",
                    product["name"], item["filename"], api_checksum, local_checksum
                )
                # Do NOT update DB if checksum fails validation
                return
            else:
                 logger.debug("Checksum validated for %s - %s", product["name"], item["filename"])

        # 4. Write File
        try:
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_bytes(file_content)
            logger.debug("Successfully wrote file: %s", path)
        except OSError as e:
            logger.error("Failed to write file %s: %s", path, e)
            return # Don't update DB if write fails

        # 5. Update Database
        now_iso = datetime.now(dt_timezone.utc).isoformat()
        api_mod_iso = product["fileLastModified"] # Already ISO string

        with self._db_conn:
            self._db_conn.execute(
                """
                INSERT INTO files (
                    product_id, item_id, filename, api_last_modified, api_checksum,
                    local_path, local_last_synced, local_checksum
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(product_id, item_id) DO UPDATE SET
                    filename = excluded.filename,
                    api_last_modified = excluded.api_last_modified,
                    api_checksum = excluded.api_checksum,
                    local_path = excluded.local_path,
                    local_last_synced = excluded.local_last_synced,
                    local_checksum = excluded.local_checksum;
                """,
                (
                    product_id, item_id, item["filename"], api_mod_iso, api_checksum,
                    str(path), now_iso, local_checksum # Store path as string
                )
            )
        logger.debug("Updated DB cache for %s - %s", product["name"], item["filename"])


    def _cleanup_db(self) -> None:
        """Remove items from DB that were not present in the last API sync."""
        if not self._touched_items:
             logger.debug("Skipping DB cleanup as no items were processed from API.")
             return

        # Create placeholders for the IN clause
        placeholders = ','.join('?' for _ in self._touched_items)
        # Flatten the set of tuples into a list of alternating product_id, item_id
        flat_keys = [val for pair in self._touched_items for val in pair]

        # Construct the query carefully to delete rows not matching the composite keys
        # This is a bit tricky with composite keys in standard SQL IN clause.
        # A safer way is to select the keys to keep and delete the rest.

        # Get all keys currently in the DB
        cursor = self._db_conn.execute("SELECT product_id, item_id FROM files")
        db_keys = {tuple(row) for row in cursor.fetchall()}

        keys_to_delete = db_keys - self._touched_items

        if keys_to_delete:
            logger.info("Removing %d orphaned item(s) from DB cache.", len(keys_to_delete))
            with self._db_conn:
                self._db_conn.executemany(
                    "DELETE FROM files WHERE product_id = ? AND item_id = ?",
                    list(keys_to_delete)
                )
            # Optional: Clean up products with no remaining files?
            # self._db_conn.execute("DELETE FROM products WHERE product_id NOT IN (SELECT DISTINCT product_id FROM files)")

    def _close_db(self) -> None:
        """Close the database connection."""
        if self._db_conn:
            self._db_conn.close()
            self._db_conn = None # type: ignore
            logger.debug("Database connection closed.")

    def __del__(self) -> None:
        # Ensure DB connection is closed if the object is garbage collected
        self._close_db()


    def _file_path(self, product: Product, item: DownloadItem) -> Path:
        publishers_name = _normalize_path_part(
            product.get("publisher", {}).get("name", "Others"), self._config.compatibility_mode
        )
        product_name = _normalize_path_part(product["name"], self._config.compatibility_mode)
        item_name = _normalize_path_part(item["filename"], self._config.compatibility_mode)
        if self._config.omit_publisher:
            return self._config.library_path / product_name / item_name
        else:
            return self._config.library_path / publishers_name / product_name / item_name


def _normalize_path_part(part: str, compatibility_mode: bool) -> str:
    """
    Strip out unwanted characters in parts of the path to the downloaded file representing
    publisher's name, product name, and item name.
    """

    # There are two algorithms for normalizing names. One is the drpg way, and the other
    # is the DriveThruRPG way.
    #
    # Normalization algorithm for DriveThruRPG's client:
    # 1. Replace any characters that are not alphanumeric, period, or space with "_"
    # 2. Replace repeated whitespace with a single space
    # # NOTE: I don't know for sure that step 2 is how their client handles it. I'm guessing.
    #
    # Normalization algorithm for drpg:
    # 1. Unescape any HTML-escaped characters (for example, convert &nbsp; to a space)
    # 2. Replace any of the characters <>:"/\|?* with " - "
    # 3. Replace any repeated " - " separators with a single " - "
    # 4. Replace repeated whitespace with a single space
    #
    # For background, this explains what characters are not allowed in filenames on Windows:
    # https://learn.microsoft.com/en-us/windows/win32/fileio/naming-a-file#naming-conventions
    # Since Windows is the lowest common denominator, we use its restrictions on all platforms.

    if compatibility_mode:
        part = PathNormalizer.normalize_drivethrurpg_compatible(part)
    else:
        part = PathNormalizer.normalize(part)
    return part


def _newest_checksum(item: DownloadItem) -> str | None:
    return max(
        item["checksums"] or [],
        default={"checksum": None},
        key=lambda s: datetime.fromisoformat(s["checksumDate"]),
    )["checksum"]


class PathNormalizer:
    separator_drpg = " - "
    multiple_drpg_separators = f"({separator_drpg})+"
    multiple_whitespaces = re.compile(r"\s+")
    non_standard_characters = re.compile(r"[^a-zA-Z0-9.\s]")

    @classmethod
    def normalize_drivethrurpg_compatible(cls, part: str) -> str:
        separator = "_"
        part = re.sub(cls.non_standard_characters, separator, part)
        part = re.sub(cls.multiple_whitespaces, " ", part)
        return part

    @classmethod
    def normalize(cls, part: str) -> str:
        separator = PathNormalizer.separator_drpg
        part = html.unescape(part)
        part = re.sub(r'[<>:"/\\|?*]', separator, part).strip(separator)
        part = re.sub(PathNormalizer.multiple_drpg_separators, separator, part)
        part = re.sub(PathNormalizer.multiple_whitespaces, " ", part)
        return part
