import json
import logging
import time
from collections.abc import Iterator
from typing import Any

import requests
from airflow.sdk import Variable
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

from dags_utils.sources.steampower.result_parsers import SteamPowerResultParser

logger = logging.getLogger(__name__)


class SteamPowerConnectionError(Exception):
    """Raised on a Steam API/network failure (bad response, timeout, refused connection)."""


class SteamPowerParameterError(Exception):
    """Raised on invalid parameters for Steam API calls."""


class SteamPowerClient:
    SEARCH_PATH = "/search/results/"
    APPDETAILS_PATH = "/api/appdetails/"
    REVIEWS_PATH = "/appreviews/"
    TAG_PATH = "/app/"
    REVIEWS_PER_PAGE = 100
    REVIEWS_PAGES = 3
    COUNTRY = ""
    LANGUAGE = "en"

    def __init__(self, timeout: int) -> None:
        self._base_url = Variable.get("steam_store_base_url").rstrip("/")
        self._timeout = timeout
        self._parser = SteamPowerResultParser
        self._session = requests.Session()
        retry = Retry(
            total=5,
            status_forcelist=[429, 500, 502, 503, 504],
            backoff_factor=5,  # 5, 10, 20, 40, 80 seconds
            allowed_methods=["GET"],
            respect_retry_after_header=True,
        )
        self._session.mount("https://", HTTPAdapter(max_retries=retry))

    def _get(self, path: str, params: dict[str, Any]) -> Any:
        url = f"{self._base_url}{path}"
        logger.info("Steam GET %s params=%s", url, params)
        try:
            response = self._session.get(url, params=params, timeout=self._timeout)
            response.raise_for_status()
            return response.json()
        except requests.RequestException as exc:
            raise SteamPowerConnectionError(f"Steam request failed: {url} params={params}") from exc

    def _get_text(self, path: str, params: dict[str, Any]) -> str:
        """Same as _get, but for store pages: they answer with HTML, not JSON."""
        url = f"{self._base_url}{path}"
        logger.info("Steam GET %s params=%s", url, params)
        try:
            response = self._session.get(url, params=params, timeout=self._timeout)
            response.raise_for_status()
            return response.text
        except requests.RequestException as exc:
            raise SteamPowerConnectionError(f"Steam request failed: {url} params={params}") from exc

    def steampower_get_total_count(self, category1: int = 998, specials: int = 0) -> int:
        """Get the number of results for a given sort_by and category1."""
        payload = self._get(
            self.SEARCH_PATH, {"infinite": 1, "category1": category1, "specials": specials}
        )

        total_count = payload.get("total_count")
        if not isinstance(total_count, int) or total_count < 0:
            raise SteamPowerConnectionError(
                f"Steam search returned no usable total_count: {total_count!r}"
            )
        return total_count

    def steampower_get_search(
        self, start: int, count: int, sort_by: str, category1: int = 998, specials: int = 0
    ) -> list[dict[str, Any]]:
        """One /search/results/ page. Returns (items sent by the server, parsed rows)."""

        payload = self._get(
            self.SEARCH_PATH,
            {
                "query": "",
                "json": 1,
                "cc": self.COUNTRY,
                "l": self.LANGUAGE,
                "start": start,
                "count": count,
                "sort_by": sort_by,
                "category1": category1,
                "specials": specials,
            },
        )
        items = payload.get("items") or []
        if not len(items):
            logger.warning("Steam search: empty page at offset=%s, walking on", start)
        return self._parser.parse_search_items(items)

    def steampower_iter_search(
        self,
        delay_seconds: float,
        count: int,
        sort_by: str,
        max_rows: int | None = None,
        specials: int = 0,
    ) -> Iterator[list[dict[str, Any]]]:
        """Walk the catalog from the newest rows backwards, yielding (offset, rows)."""

        if max_rows is not None:
            offsets = range(0, max_rows, count)
        else:
            total_count = self.steampower_get_total_count(specials=specials)
            offsets = range(0, total_count, count)
            logger.info("Steam search: total_count=%s, reading %s pages", total_count, len(offsets))

        for offset in offsets:
            if offset > 0 and delay_seconds:
                time.sleep(delay_seconds)

            rows = self.steampower_get_search(
                start=offset, count=count, sort_by=sort_by, specials=specials
            )
            yield rows

    def steampower_get_appdetails(
        self,
        delay_seconds: float,
        appids: list[int],
        batch_size: int = 500,
    ) -> Iterator[tuple[list[dict[str, Any]], list[dict[str, Any]], list[dict[str, Any]]]]:
        """Read /api/appdetails/ per appid, yielding (appdetails, price, packages) row batches."""
        if not appids:
            raise SteamPowerParameterError("appids must not be empty")
        if min(appids) < 1:
            raise SteamPowerParameterError(f"appid must be >= 1, got {appids}")

        appdetails_lst: list[dict[str, Any]] = []
        price_lst: list[dict[str, Any]] = []
        packages_lst: list[dict[str, Any]] = []

        logger.info("Steam appdetails: reading %s appids, batch_size=%s", len(appids), batch_size)

        for position, appid in enumerate(appids):
            if position > 0 and delay_seconds:
                time.sleep(delay_seconds)
            try:
                result = self._get(
                    self.APPDETAILS_PATH,
                    {
                        "appids": appid,
                        "cc": self.COUNTRY,
                        "l": self.LANGUAGE,
                    },
                )
            except SteamPowerConnectionError:
                logger.warning("Steam appdetails: giving up on appid=%s", appid)
                appdetails_lst.append(self._parser.build_appdetails_row(appid, {}))
                continue

            raw = result.get(str(appid)) or {}
            data = raw.get("data") or {}

            appdetails_lst.append(self._parser.build_appdetails_row(appid, raw))
            if raw.get("success"):
                price_lst.append(self._parser.build_price_row(appid, data))
                packages_lst.extend(self._parser.build_packages_rows(appid, data))
            else:
                logger.warning("Steam appdetails: no data for appid=%s", appid)

            if len(appdetails_lst) >= batch_size:
                yield appdetails_lst, price_lst, packages_lst
                appdetails_lst, price_lst, packages_lst = [], [], []

        if appdetails_lst:
            yield appdetails_lst, price_lst, packages_lst

    def steampower_get_dlc(
        self,
        delay_seconds: float,
        appids: list[int],
        batch_size: int = 500,
    ) -> Iterator[tuple[list[dict[str, Any]], list[dict[str, Any]], list[dict[str, Any]]]]:
        """Read /api/appdetails/ per add-on appid, yielding (details, price, packages) batches."""
        if not appids:
            raise SteamPowerParameterError("appids must not be empty")
        if min(appids) < 1:
            raise SteamPowerParameterError(f"appid must be >= 1, got {appids}")

        details_lst: list[dict[str, Any]] = []
        price_lst: list[dict[str, Any]] = []
        packages_lst: list[dict[str, Any]] = []

        logger.info("Steam dlc: reading %s appids, batch_size=%s", len(appids), batch_size)

        for position, dlc_appid in enumerate(appids):
            if position > 0 and delay_seconds:
                time.sleep(delay_seconds)
            try:
                result = self._get(
                    self.APPDETAILS_PATH,
                    {
                        "appids": dlc_appid,
                        "cc": self.COUNTRY,
                        "l": self.LANGUAGE,
                    },
                )
            except SteamPowerConnectionError:
                logger.warning("Steam dlc: giving up on dlc_appid=%s", dlc_appid)
                continue

            raw = result.get(str(dlc_appid)) or {}
            data = raw.get("data") or {}

            if not raw.get("success") or not data.get("fullgame", {}).get("appid"):
                logger.warning("Steam dlc: no data for dlc_appid=%s", dlc_appid)
                continue

            details_lst.append(self._parser.build_dlc_details_row(dlc_appid, data))
            price_lst.append(self._parser.build_dlc_price_row(dlc_appid, data))
            packages_lst.extend(self._parser.build_dlc_packages_rows(dlc_appid, data))

            if len(details_lst) >= batch_size:
                yield details_lst, price_lst, packages_lst
                details_lst, price_lst, packages_lst = [], [], []

        if details_lst:
            yield details_lst, price_lst, packages_lst

    def steampower_get_appreviews(
        self,
        delay_seconds: float,
        appids: list[int],
        batch_size: int = 500,
    ) -> Iterator[tuple[list[dict[str, Any]], list[dict[str, Any]]]]:
        """Read /appreviews/"""
        if not appids:
            raise SteamPowerParameterError("appids must not be empty")
        if min(appids) < 1:
            raise SteamPowerParameterError(f"appid must be >= 1, got {appids}")

        appreviews_lst: list[dict[str, Any]] = []
        appreviews_details_lst: list[dict[str, Any]] = []

        logger.info("Steam appreviews: reading %s appids, batch_size=%s", len(appids), batch_size)

        for appid in appids:
            cursor_prev = ""
            cursor = "*"
            first_page: dict[str, Any] = {}
            for page in range(self.REVIEWS_PAGES):
                if cursor_prev == cursor:
                    break

                try:
                    result = self._get(
                        self.REVIEWS_PATH + str(appid),
                        {
                            "json": 1,
                            "cc": self.COUNTRY,
                            "l": self.LANGUAGE,
                            "filter": "recent",
                            "num_per_page": self.REVIEWS_PER_PAGE,
                            "cursor": cursor,
                        },
                    )
                except SteamPowerConnectionError:
                    # Keep the pages already fetched for this game and move on to the next one.
                    logger.warning("Steam appreviews: page %s failed for appid=%s", page, appid)
                    break
                # get summary appdetails
                if page == 0:
                    first_page = result

                cursor_prev = cursor
                cursor = result.get("cursor")
                appreviews_details_lst.extend(
                    self._parser.build_appreviews_details_rows(appid, result)
                )

                if delay_seconds:
                    time.sleep(delay_seconds)

            # First page failed: an all-NULL summary would read as a fake state change.
            if not first_page:
                continue
            appreviews_lst.append(self._parser.build_appreviews_row(appid, first_page))

            if len(appreviews_lst) >= batch_size:
                yield appreviews_lst, appreviews_details_lst
                appreviews_lst, appreviews_details_lst = [], []

        if appreviews_lst:
            yield appreviews_lst, appreviews_details_lst

    def steampower_get_apptag(
        self,
        delay_seconds: float,
        appids: list[int],
        batch_size: int = 500,
    ) -> Iterator[list[dict[str, Any]]]:
        """Read the user tags embedded in the store page /app/<appid>/."""
        if not appids:
            raise SteamPowerParameterError("appids must not be empty")
        if min(appids) < 1:
            raise SteamPowerParameterError(f"appid must be >= 1, got {appids}")

        apptag_lst: list[dict[str, Any]] = []
        updated_appids: list[dict[str, Any]] = []

        logger.info("Steam app tag: reading %s appids, batch_size=%s", len(appids), batch_size)

        for position, appid in enumerate(appids):
            if position > 0 and delay_seconds:
                time.sleep(delay_seconds)
            try:
                page = self._get_text(
                    self.TAG_PATH + str(appid),
                    {
                        "cc": self.COUNTRY,
                        "l": self.LANGUAGE,
                    },
                )

                tags = self._parser.parse_apptag_page(appid, page)
            except (SteamPowerConnectionError, json.JSONDecodeError):
                logger.warning("Steam app tag: no tags for appid=%s", appid)
                tags = [{}]

            apptag_lst.extend(self._parser.build_apptag_rows(appid, tags))
            updated_appids.append(appid)
            if len(updated_appids) >= batch_size:
                yield apptag_lst
                apptag_lst = []
                updated_appids = []

        if apptag_lst:
            yield apptag_lst
