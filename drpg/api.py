from __future__ import annotations

import logging
from time import sleep
from typing import TYPE_CHECKING

import cloudscraper
import requests

from drpg.types import PrepareDownloadUrlResponse

if TYPE_CHECKING:  # pragma: no cover
    from collections.abc import Iterator

    from drpg.types import Product, TokenResponse

logger = logging.getLogger("drpg")
JSON_MIME = "application/json"


class DrpgApi:
    """Low-level REST API client for DriveThruRPG"""

    API_URL = "https://api.drivethrurpg.com/api/vBeta"

    class PrepareDownloadUrlException(Exception):
        UNEXPECTED_RESPONSE = "Got response with unexpected schema"
        REQUEST_FAILED = "Got non 2xx response"

    def __init__(self, api_key: str):
        logger.debug("Preparing cloudscraper client")
        self._client = cloudscraper.create_scraper()
        self._client.headers["Content-Type"] = JSON_MIME
        self._client.headers["Accept"] = JSON_MIME
        self._client.headers["Accept-Encoding"] = "gzip,deflate"
        self._client.headers["Connection"] = "keep-alive"
        self._api_key = api_key

    def token(self) -> TokenResponse:
        """Authenticate http client with access token based on an API key."""
        resp = self._client.post(
            f"{self.API_URL}/auth_key",
            params={"applicationKey": self._api_key},
        )

        if resp.status_code == requests.codes.UNAUTHORIZED:
            raise AttributeError("Provided token is invalid")

        login_data: TokenResponse = resp.json()
        self._client.headers["Authorization"] = login_data["token"]
        return login_data

    def customer_products(self, per_page: int = 50) -> Iterator[Product]:
        """List all not archived customer's products."""

        page = 1

        while result := self._product_page(page, per_page):
            logger.debug("Yielding products page %d", page)
            yield from result
            page += 1

    def prepare_download_url(self, product_id: int, item_id: int) -> PrepareDownloadUrlResponse:
        """Generate a download link and metadata for a product's item."""

        task_params = {
            "siteId": 10,  # Magic number, probably something like storefront ID
            "index": item_id,
            "getChecksums": 0,  # Official clients defaults to 1
        }
        resp = self._client.get(f"{self.API_URL}/order_products/{product_id}/prepare", params=task_params)

        def _parse_message(resp) -> PrepareDownloadUrlResponse:
            message: PrepareDownloadUrlResponse = resp.json()
            if resp.ok:
                expected_keys = PrepareDownloadUrlResponse.__required_keys__
                if isinstance(message, dict) and expected_keys.issubset(message.keys()):
                    logger.debug("Got download url for %s - %s: %s", product_id, item_id, message)
                else:
                    logger.debug(
                        "Got unexpected message when getting download url for %s - %s: %s",
                        product_id,
                        item_id,
                        message,
                    )
                    raise self.PrepareDownloadUrlException(
                        self.PrepareDownloadUrlException.UNEXPECTED_RESPONSE
                    )
            else:
                logger.debug(
                    "Could not get download link for %s - %s: %s",
                    product_id,
                    item_id,
                    message,
                )
                raise self.PrepareDownloadUrlException(
                    self.PrepareDownloadUrlException.REQUEST_FAILED
                )
            return message

        while (data := _parse_message(resp))["status"].startswith("Preparing"):
            logger.debug("Waiting for download link for: %s - %s", product_id, item_id)
            sleep(2)
            resp = self._client.get(f"{self.API_URL}/order_products/{product_id}/check", params=task_params)

        logger.debug("Got download link for: %s - %s", product_id, item_id)
        return data

    def _product_page(self, page: int, per_page: int) -> list[Product]:
        """List products from a specified page."""

        return self._client.get(
            f"{self.API_URL}/order_products",
            params={
                "getChecksum": 1,
                "getFilters": 0,  # Official clients defaults to 1
                "page": page,
                "pageSize": per_page,
                "library": 1,
                "archived": 0,
            },
        ).json()
