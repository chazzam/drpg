from __future__ import annotations

import functools
import html
import logging
import re
import sqlite3

# from datetime import datetime, timedelta, timezone as dt_timezone
from datetime import datetime, timezone as dt_timezone
from hashlib import md5
from multiprocessing.pool import ThreadPool

# from time import timezone  # Keep this for the timedelta calculation, maybe rename later
from typing import TYPE_CHECKING, NamedTuple

import httpx

from drpg.api import DrpgApi
from drpg.custom_types import Publisher, DownloadItem, Product, Checksum  # Import this type

if TYPE_CHECKING:  # pragma: no cover
    from collections.abc import Iterator
    from pathlib import Path
    from typing import Any, Callable

    from drpg.config import Config

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
    validated: bool | None


class DbProductInfo(NamedTuple):
    product_id: int | None
    name: str | None
    publisher_name: str | None
    last_api_check: str | None


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
    """
    High level DriveThruRPG client that syncs products from a customer's library using a DB cache.
    """

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
        validated INTEGER NOT NULL,
        PRIMARY KEY (product_id, item_id),
        FOREIGN KEY (product_id) REFERENCES products(product_id) ON DELETE CASCADE
    );

    CREATE INDEX IF NOT EXISTS idx_files_local_path ON files (local_path);
    """

    def __init__(self, config: Config) -> None:
        self._config = config
        self._api = DrpgApi(config.token)
        self._db_conn = sqlite3.connect(config.db_path, isolation_level=None)  # Autocommit mode
        self._db_conn.row_factory = sqlite3.Row  # Access columns by name
        self._db_conn.execute("PRAGMA foreign_keys = ON;")
        self._setup_db()
        self._touched_items = set()  # Keep track of items seen in current API sync
        self._page_update = 50

    def _setup_db(self) -> None:
        """Create database tables if they don't exist."""
        if not self._db_conn:
            return
        with self._db_conn:
            self._db_conn.executescript(self._DB_SCHEMA)
        logger.debug("Database schema initialized at %s", self._config.db_path)

    def _get_db_products_info(self) -> Iterator[Product]:
        """Fetch file metadata from the database."""
        if not self._db_conn:
            return
        cursor = self._db_conn.execute(
            """
            SELECT name, publisher_name, name, product_id, last_api_check
            FROM products
            """,
        )
        rows = cursor.fetchall()
        if not rows:
            rows = []

        prods = []
        for row in rows:
            name = str(row["name"])
            pub = str(row["publisher_name"])
            order = int(row["product_id"])
            lastMod = str(row["last_api_check"])
            cursor = self._db_conn.execute(
                """
                SELECT item_id, local_path, api_checksum, api_last_modified
                FROM files
                WHERE product_id = ?
                """,
                [order],
            )
            items = cursor.fetchall()
            if not items:
                items = []
            files = []
            for item in items:
                files.append(
                    DownloadItem(
                        index=item["item_id"],
                        filename=item["local_path"],
                        checksums=[
                            Checksum(
                                checksum=item["api_checksum"],
                                checksumDate=item["api_last_modified"],
                            )
                        ],
                    )
                )
            prods.append(
                Product(
                    productId=name,
                    publisher=Publisher(name=pub),
                    name=name,
                    orderProductId=order,
                    fileLastModified=lastMod,
                    files=files,
                )
            )
        yield from prods

    def sync(self) -> None:
        files = self.get_sync_list()
        self.process_items(files)

    def get_sync_list(self) -> list:
        """Download all new, updated and not yet synced items to a sync directory using DB cache."""
        logger.info("Authenticating")
        self._api.token()

        # Prepare arguments for parallel processing
        process_item_args = []
        print(f"  use cache: {self._config.use_cached_products}")
        if not self._config.use_cached_products:
            logger.info("Fetching products list from API")
            self._touched_items.clear()  # Reset for this sync run

            try:
                count = 0
                print(".", end="", flush=True)
                per_page = self._page_update
                for product in self._api.customer_products(per_page=per_page):
                    if count % per_page == 0:
                        print(".", end="", flush=True)
                    count += 1
                    # Update product info in DB (or insert if new)
                    self._update_product_in_db(product)
                    for item in product["files"]:
                        item_key = (product["orderProductId"], item["index"])
                        self._touched_items.add(item_key)  # Mark as seen in API
                        api_checksum = _newest_checksum(item)
                        # Cache this file entry
                        self._update_file_db(
                            product, item, self._db_conn, False, api_checksum, None, True
                        )
                        process_item_args.append((product, item))
            except Exception as e:
                logger.error("Failed to fetch products from API: %s", e, exc_info=True)
                self._close_db()
                return []
            print(".")
        else:
            # Loop over product files cached in the db and add them for processing
            for prod in self._get_db_products_info():
                for file in prod["files"]:
                    process_item_args.append((prod, file))
        return process_item_args

    def process_items(self, process_item_args) -> None:
        """Download all new, updated and not yet synced items to a sync directory using DB cache."""
        logger.info("Checking %d items against local cache/filesystem", len(process_item_args))
        # Use ThreadPool for downloading, but decisions are made sequentially before this
        items_to_download = []
        count = 0
        for product, item in process_item_args:
            if count % self._page_update == 0:
                print(".", end="", flush=True)
            count += 1
            if self._need_download_db(product, item):
                items_to_download.append((product, item))
        print(".")

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
        if not self._db_conn:
            return
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

    def _cache_product_in_db(self, product: Product) -> None:
        """Insert or update product information in the database."""
        if not self._db_conn:
            return
        now = datetime.now(dt_timezone.utc).isoformat()
        publisher_name = product.get("publisher", {}).get("name")
        with self._db_conn:
            self._db_conn.execute(
                """
                INSERT INTO products (product_id, name, publisher_name, last_api_check)
                VALUES (?, ?, ?, ?)
                ON CONFLICT(product_id) DO NOTHING;
                """,
                (product["orderProductId"], product["name"], publisher_name, now),
            )

    def _get_db_file_info(self, product_id: int, item_id: int) -> DbFileInfo | None:
        """Fetch file metadata from the database."""
        if not self._db_conn:
            return
        cursor = self._db_conn.execute(
            """
            SELECT api_last_modified, api_checksum, local_path, local_last_synced, local_checksum, validated
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
            # If not using the checksum, we won't download it again if it exists.
            # Just add it to the db instead
            if expected_path.exists():
                local_checksum = md5(expected_path.read_bytes()).hexdigest()
                api_checksum = _newest_checksum(item)
                if not self._config.use_checksums and not self._config.validate:
                    self._update_file_db(
                        product, item, self._db_conn, False, api_checksum, local_checksum
                    )
                    logger.info(
                        "File exists, not updating: %s - %s", product["name"], item["filename"]
                    )
                    return False

                if api_checksum != local_checksum:
                    logger.debug(
                        "Needs download: %s - %s: checksum failure ('%s' vs '%s')",
                        product["name"],
                        item["filename"],
                        local_checksum,
                        api_checksum,
                    )
                    return True

                self._update_file_db(
                    product, item, self._db_conn, True, api_checksum, local_checksum
                )
                logger.info("File exists, Up to date: %s - %s", product["name"], item["filename"])
                return False
            else:
                logger.debug(
                    "Needs download: %s - %s: No record in DB cache",
                    product["name"],
                    item["filename"],
                )
                return True

        # Check if path changed due to config
        if str(expected_path) != db_info.local_path:
            logger.debug(
                "Needs download: %s - %s: Local path changed ('%s' vs '%s')",
                product["name"],
                item["filename"],
                db_info.local_path,
                expected_path,
            )
            return True

        # Check API modification time against DB cache
        api_mod_time_str = product["fileLastModified"]
        if api_mod_time_str != db_info.api_last_modified:
            logger.debug(
                "Needs download: %s - %s: API modification time changed ('%s' vs '%s')",
                product["name"],
                item["filename"],
                db_info.api_last_modified,
                api_mod_time_str,
            )
            return True

        # Check checksum if enabled
        if self._config.use_checksums:
            api_checksum = _newest_checksum(item)
            if api_checksum != db_info.api_checksum or not expected_path.exists():
                logger.debug(
                    "Needs download: %s - %s: API checksum changed ('%s' vs '%s')",
                    product["name"],
                    item["filename"],
                    db_info.api_checksum,
                    api_checksum,
                )
                return True
            # If we never validated the file against the checksum,
            # we don't know if the file matches yet
            if not db_info.validated and expected_path.exists():
                local_checksum = md5(expected_path.read_bytes()).hexdigest()
                if api_checksum != local_checksum:
                    logger.debug(
                        "Needs download: %s - %s: checksum failure ('%s' vs '%s')",
                        product["name"],
                        item["filename"],
                        local_checksum,
                        api_checksum,
                    )
                    return True
                # We have now validated the download, so update the db as such
                self._update_file_db(
                    product, item, self._db_conn, True, api_checksum, local_checksum
                )
                return False

        # Fallback: Check if file actually exists at the cached path (maybe deleted manually)
        # This adds a stat call but prevents errors if DB is out of sync with reality.
        if not expected_path.exists():
            logger.debug(
                "Needs download: %s - %s: File missing at cached path '%s'",
                product["name"],
                item["filename"],
                expected_path,
            )
            return True

        logger.info("Up to date (cached): %s - %s", product["name"], item["filename"])
        return False

    def _update_file_db(
        self,
        product: Product,
        item: DownloadItem,
        db_conn: Connection,
        validated: bool,
        api_checksum: str | None,
        local_checksum: str | None,
        cache: bool = False,
    ) -> None:
        product_id = product["orderProductId"]
        item_id = item["index"]
        path = self._file_path(product, item)
        # 5. Update Database
        api_mod_iso = product["fileLastModified"]  # Already ISO string
        datestamp = datetime.now(dt_timezone.utc).isoformat()
        query = """
                INSERT INTO files (
                    product_id, item_id, filename, api_last_modified, api_checksum,
                    local_path, local_last_synced, local_checksum, validated
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(product_id, item_id) DO UPDATE SET
                    filename = excluded.filename,
                    api_last_modified = excluded.api_last_modified,
                    api_checksum = excluded.api_checksum,
                    local_path = excluded.local_path,
                    local_last_synced = excluded.local_last_synced,
                    local_checksum = excluded.local_checksum,
                    validated = excluded.validated;
                """
        if cache:
            datestamp = datetime.min.isoformat()
            query = """
                INSERT INTO files (
                    product_id, item_id, filename, api_last_modified, api_checksum,
                    local_path, local_last_synced, local_checksum, validated
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(product_id, item_id) DO NOTHING
                """

        with db_conn:
            db_conn.execute(
                query,
                (
                    product_id,
                    item_id,
                    item["filename"],
                    api_mod_iso,
                    api_checksum,
                    str(path),
                    datestamp,
                    local_checksum,
                    validated,  # Store path as string
                ),
            )
        if not cache:
            logger.debug("Updated DB cache for %s - %s", product["name"], item["filename"])

    def _get_item_url_db(self, product: Product, item: DownloadItem) -> str:
        """Download an item and update the database cache."""
        product_id = product["orderProductId"]
        item_id = item["index"]
        # 1. Get Download URL
        url_data = ""
        try:
            tmp = self._api.prepare_download_url(product_id, item_id)
            if tmp:
                url_data = tmp["url"]
        except self._api.PrepareDownloadUrlException as e:
            logger.warning(
                "Could not get download URL for %s - %s: %s", product["name"], item["filename"], e
            )
            # Don't update DB if we can't even get the URL
            url_data = ""
        if not url_data:
            url_data = ""
        return url_data

    def _download_item_url(self, product: Product, item: DownloadItem, url: str):
        """Download an item and update the database cache."""
        file_content = ""
        if not url:
            return None
        try:
            file_response = httpx.get(
                url,
                timeout=60.0,  # Slightly longer timeout for downloads
                follow_redirects=True,
                headers={
                    "Accept-Encoding": "gzip, deflate, br",
                    "User-Agent": "Mozilla/5.0",
                    "Accept": "*/*",
                },
            )
            file_response.raise_for_status()  # Raise exception for bad status codes
            file_content = file_response.content
        except httpx.HTTPStatusError as e:
            logger.error(
                "HTTP error downloading %s - %s: %s", product["name"], item["filename"], e
            )
            return None
        except httpx.RequestError as e:
            logger.error(
                "Network error downloading %s - %s: %s", product["name"], item["filename"], e
            )
            return None
        return file_content

    def _validate_item_content(self, product: Product, item: DownloadItem, file_content):
        """Download an item and update the database cache."""
        local_checksum = None
        api_checksum = _newest_checksum(item)
        validated = False
        if self._config.validate and api_checksum:
            local_checksum = md5(file_content).hexdigest()
            if local_checksum != api_checksum:
                logger.error(
                    "ERROR: Invalid checksum for %s - %s,"
                    " skipping saving file (API: %s != Local: %s)",
                    product["name"],
                    item["filename"],
                    api_checksum,
                    local_checksum,
                )
                # Do NOT update DB if checksum fails validation
            else:
                validated = True
                logger.debug("Checksum validated for %s - %s", product["name"], item["filename"])
        else:
            # If we're not validating, it passes!
            validated = True
        return (validated, api_checksum, local_checksum)

    @suppress_errors(
        httpx.HTTPError, PermissionError, sqlite3.Error, DrpgApi.PrepareDownloadUrlException
    )
    def _process_item_db(self, product: Product, item: DownloadItem) -> None:
        """Download an item and update the database cache."""
        path = self._file_path(product, item)

        if self._config.dry_run:
            logger.info("DRY RUN - would have downloaded file: %s", path)
            # In dry run, maybe update DB as if downloaded? Or skip DB update?
            # Let's skip DB update for dry run to keep it simple.
            return

        logger.info("Processing: %s - %s", product["name"], item["filename"])

        # 1. Get Download URL
        url = self._get_item_url_db(product, item)

        # 2. Download File
        file_content = self._download_item_url(product, item, url)
        if not file_content:
            return

        # 3. Validate Checksum (if enabled)
        (validated, api_checksum, local_checksum) = self._validate_item_content(
            product, item, file_content
        )

        if not validated:
            return

        # 4. Write File
        try:
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_bytes(file_content)
            logger.debug("Successfully wrote file: %s", path)
        except OSError as e:
            logger.error("Failed to write file %s: %s", path, e)
            return  # Don't update DB if write fails

        db_conn = sqlite3.connect(self._config.db_path, isolation_level=None)  # Autocommit mode
        if not self._db_conn:
            return
        db_conn.row_factory = sqlite3.Row  # Access columns by name
        db_conn.execute("PRAGMA foreign_keys = ON;")
        self._update_file_db(product, item, db_conn, validated, api_checksum, local_checksum)
        if db_conn:
            db_conn.close()
            db_conn = None  # type: ignore

    def _cleanup_db(self) -> None:
        """Remove items from DB that were not present in the last API sync."""
        if not self._touched_items:
            logger.debug("Skipping DB cleanup as no items were processed from API.")
            return
        if not self._db_conn:
            return

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
                    "DELETE FROM files WHERE product_id = ? AND item_id = ?", list(keys_to_delete)
                )
            # Optional: Clean up products with no remaining files?
            # self._db_conn.execute("DELETE FROM products
            # WHERE product_id NOT IN (SELECT DISTINCT product_id FROM files)")

    def _close_db(self) -> None:
        """Close the database connection."""
        if self._db_conn:
            self._db_conn.close()
            self._db_conn = None  # type: ignore
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
