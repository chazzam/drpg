import re
from functools import partial
from unittest import TestCase, mock
from urllib.parse import urlencode, urlparse

import requests_mock

from drpg import api

from .fixtures import PrepareDownloadUrlResponseFixture

_api_url = urlparse(api.DrpgApi.API_URL)
api_base_url = f"{_api_url.scheme}://{_api_url.hostname}"

class DrpgApiTokenTest(TestCase):
    def setUp(self):
        self.login_url = f"{api_base_url}/api/vBeta/auth_key"
        self.client = api.DrpgApi("token")

    @requests_mock.Mocker()
    def test_login_valid_token(self, mock):
        content = {"token": "some-token", "refreshToken": "123", "refreshTokenTTL": 171235}
        mock.post(self.login_url, json=content)

        login_data = self.client.token()
        self.assertEqual(login_data, content)

    @requests_mock.Mocker()
    def test_login_invalid_token(self, mock):
        mock.post(self.login_url, status_code=401)

        with self.assertRaises(AttributeError):
            self.client.token()


class DrpgApiCustomerProductsTest(TestCase):
    def setUp(self):
        url = f"{api_base_url}/api/vBeta/order_products"
        self.products_page = re.compile(f"{url}\\?.+$")
        self.client = api.DrpgApi("token")

    @requests_mock.Mocker()
    def test_one_page(self, mock):
        page_1_products = [{"name": "First Product"}]
        mock.get(self.products_page, [
                {'json':page_1_products},
                {'json':[]},
            ]
        )

        products = self.client.customer_products()
        self.assertEqual(list(products), page_1_products)

    @requests_mock.Mocker()
    def test_multiple_pages(self, mock):
        page_1_products = [{"name": "First Product"}]
        page_2_products = [{"name": "Second Product"}]
        mock.get(self.products_page, [
                {'json':page_1_products},
                {'json':page_2_products},
                {'json':[]},
            ]
        )
        products = self.client.customer_products()
        self.assertEqual(list(products), page_1_products + page_2_products)


class DrpgApiPrepareDownloadUrlTest(TestCase):
    def setUp(self):
        self.order_product_id = 123
        params = urlencode({"siteId": 10, "index": 0, "getChecksums": 0})
        self.prepare_download_url = (
            f"{api_base_url}/api/vBeta/order_products/{self.order_product_id}/prepare?{params}"
        )
        self.check_download_url = (
            f"{api_base_url}/api/vBeta/order_products/{self.order_product_id}/check?{params}"
        )

        self.response_preparing = PrepareDownloadUrlResponseFixture.preparing()
        self.response_ready = PrepareDownloadUrlResponseFixture.complete()

        self.client = api.DrpgApi("token")

    @requests_mock.Mocker()
    def test_immiediate_download_url(self, mock):
        mock.get(self.prepare_download_url, json=self.response_ready)

        file_data = self.client.prepare_download_url(self.order_product_id, 0)
        self.assertEqual(file_data, self.response_ready)

    @requests_mock.Mocker()
    def test_wait_for_download_url(self, mock):
        mock.get(self.prepare_download_url, json=self.response_preparing)
        mock.get(self.check_download_url, json=self.response_ready)

        file_data = self.client.prepare_download_url(self.order_product_id, 0)
        self.assertEqual(file_data, self.response_ready)

    @requests_mock.Mocker()
    def test_unsuccesful_response(self, mock):
        dummy_400_response = {"message": "Invalid product id"}
        mock.get(self.prepare_download_url, status_code=400, json=dummy_400_response)

        with self.assertRaises(self.client.PrepareDownloadUrlException) as cm:
            self.client.prepare_download_url(self.order_product_id, 0)
        self.assertTupleEqual(
            cm.exception.args, (self.client.PrepareDownloadUrlException.REQUEST_FAILED,)
        )

    @requests_mock.Mocker()
    def test_unexpected_response_with_string_message(self, mock):
        dummy_unexpected_response = {"message": "Invalid product id"}
        mock.get(self.prepare_download_url, json=dummy_unexpected_response)

        with self.assertRaises(self.client.PrepareDownloadUrlException) as cm:
            self.client.prepare_download_url(self.order_product_id, 0)
        self.assertTupleEqual(
            cm.exception.args, (self.client.PrepareDownloadUrlException.UNEXPECTED_RESPONSE,)
        )

    @requests_mock.Mocker()
    def test_unexpected_response_with_json_message(self, mock):
        dummy_unexpected_response = {"message": {"reason": "Invalid product id"}}
        mock.get(self.prepare_download_url, json=dummy_unexpected_response)

        with self.assertRaises(self.client.PrepareDownloadUrlException) as cm:
            self.client.prepare_download_url(self.order_product_id, 0)
        self.assertTupleEqual(
            cm.exception.args, (self.client.PrepareDownloadUrlException.UNEXPECTED_RESPONSE,)
        )
