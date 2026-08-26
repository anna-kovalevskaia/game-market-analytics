import json
import logging
import re
import time
from collections.abc import Iterator
from datetime import datetime
from typing import Any

import requests
from airflow.sdk import Variable
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

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
    APPID_RE = re.compile(r"/apps/(\d+)/")
    APPTAG_RE = re.compile(r"InitAppTagModal\(\s*\d+\s*,\s*(\[.*?\])\s*,", re.S)
    REVIEWS_PER_PAGE = 0
    COUNTRY = ""
    LANGUAGE = "en"
    DATE_FORMATS = ("%b %d, %Y", "%d %b, %Y", "%d %B, %Y", "%B %d, %Y")

    def __init__(self, timeout: int) -> None:
        self._base_url = Variable.get("steam_store_base_url").rstrip("/")
        self._timeout = timeout
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

    @classmethod
    def _try_date_parse(cls, raw_date: str) -> datetime | None:
        """Try to parse a date string into a datetime object."""
        for fmt in cls.DATE_FORMATS:
            try:
                return datetime.strptime(raw_date or "", fmt)
            except ValueError:
                continue
        if raw_date:
            logger.debug(
                "Steam GET failed to parse date string %s into a datetime object",
                raw_date,
            )
        return None

    @classmethod
    def _parse_search_items(cls, items: list[dict[str, Any]]) -> list[dict[str, Any]]:
        """Pull appid and name out of the search result items."""
        rows: list[dict[str, Any]] = []
        for item in items:
            match = cls.APPID_RE.search(item.get("logo") or "")
            if match is None:
                # /subs/ and /bundles/ links carry no appid — skip, but say so.
                logger.warning("Steam search: no appid in logo, name=%r", item.get("name"))
                continue
            rows.append({"appid": int(match.group(1)), "name": item.get("name") or ""})
        logger.info("Steam search: ok, rows=%s", len(rows))
        return rows

    @classmethod
    def _build_appdetails_row(cls, appid: int, raw: dict[str, Any]) -> dict[str, Any]:
        """One raw.steampower_appdetails row out of an /api/appdetails/ entry."""
        data = raw.get("data") or {}
        categories = data.get("categories") or []
        genres = data.get("genres") or []
        return {
            "success": raw.get("success", False),
            "appid": appid,
            "name": data.get("name", ""),
            "required_age": data.get("required_age", None),
            "is_free": data.get("is_free", None),
            "supported_languages": data.get("supported_languages", ""),
            "website": data.get("website", ""),
            "pc_requirements": (data.get("pc_requirements") or {}).get("minimum", ""),
            "mac_requirements": (data.get("mac_requirements") or {}).get("minimum", ""),
            "linux_requirements": (data.get("linux_requirements") or {}).get("minimum", ""),
            "developers": data.get("developers", []),
            "publishers": data.get("publishers", []),
            "categories_id": [ctgr_data.get("id") for ctgr_data in categories],
            "categories_description": [ctgr_data.get("description") for ctgr_data in categories],
            "genres_id": [genres_data.get("id") for genres_data in genres],
            "genres_description": [genres_data.get("description") for genres_data in genres],
            "release_date": cls._try_date_parse((data.get("release_date") or {}).get("date", "")),
        }

    @classmethod
    def _build_price_row(cls, appid: int, data: dict[str, Any]) -> dict[str, Any]:
        """One raw.steampower_price row. price_overview is absent for free apps."""
        price = data.get("price_overview") or {}
        return {
            "appid": appid,
            "name": data.get("name", ""),
            "currency": price.get("currency", ""),
            "price_initial": price.get("initial", None),
            "price_final": price.get("final", None),
            "discount_percent": price.get("discount_percent", None),
        }

    @classmethod
    def _build_packages_rows(cls, appid: int, data: dict[str, Any]) -> list[dict[str, Any]]:
        """Flatten package_groups[].subs[] into raw.steampower_packages rows."""
        return [
            {
                "appid": appid,
                "name": data.get("name", ""),
                "packageid": pckg_data.get("packageid"),
                "package_option_text": pckg_data.get("option_text", ""),
                "package_price_with_discount": pckg_data.get("price_in_cents_with_discount", None),
            }
            for group in data.get("package_groups") or []
            for pckg_data in group.get("subs") or []
        ]

    @classmethod
    def _build_appreviews_row(cls, appid: int, data: dict[str, Any]) -> dict[str, Any]:
        """One raw.steampower_appreviews row out of the /appreviews/ query_summary."""
        query_summary = data.get("query_summary") or {}
        return {
            "appid": appid,
            "review_score": query_summary.get("review_score", None),
            "review_score_desc": query_summary.get("review_score_desc", None),
            "total_positive": query_summary.get("total_positive", None),
            "total_negative": query_summary.get("total_negative", None),
            "total_reviews": query_summary.get("total_reviews", None),
        }

    @classmethod
    def _build_apptag_rows(cls, appid: int, data: list[dict[str, Any]]) -> list[dict[str, Any]]:
        """One raw.steampower_apptag row per user tag of the game."""
        return [
            {
                "appid": appid,
                "tagid": row.get("tagid", None),
                "tag_name": row.get("name", None),
                "count": row.get("count", None),
                "browseable": row.get("browseable", None),
            }
            for row in data
        ]

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
        if start < 0:
            raise SteamPowerParameterError(f"start must be >= 0, got {start}")

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
        return self._parse_search_items(items)

    def steampower_iter_search(
        self,
        delay_seconds: float,
        count: int,
        sort_by: str,
        max_rows: int | None = None,
        specials: int = 0,
    ) -> Iterator[list[dict[str, Any]]]:
        """Walk the catalog from the newest rows backwards, yielding (offset, rows)."""
        if delay_seconds < 0:
            raise SteamPowerParameterError(f"delay_seconds must be >= 0, got {delay_seconds}")
        if count < 1:
            raise SteamPowerParameterError(f"count must be >= 1, got {count}")
        if max_rows is not None and max_rows < 1:
            raise SteamPowerParameterError(f"max_rows must be >= 1 or None, got {max_rows}")

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
                appdetails_lst.append(self._build_appdetails_row(appid, {}))
                continue

            raw = result.get(str(appid)) or {}
            data = raw.get("data") or {}

            appdetails_lst.append(self._build_appdetails_row(appid, raw))
            if raw.get("success"):
                price_lst.append(self._build_price_row(appid, data))
                packages_lst.extend(self._build_packages_rows(appid, data))
            else:
                logger.warning("Steam appdetails: no data for appid=%s", appid)

            if len(appdetails_lst) >= batch_size:
                yield appdetails_lst, price_lst, packages_lst
                appdetails_lst, price_lst, packages_lst = [], [], []

        if appdetails_lst:
            yield appdetails_lst, price_lst, packages_lst

    def steampower_get_appreviews(
        self,
        delay_seconds: float,
        appids: list[int],
        batch_size: int = 500,
    ) -> Iterator[list[dict[str, Any]]]:
        """Read /appreviews/"""
        if not appids:
            raise SteamPowerParameterError("appids must not be empty")
        if min(appids) < 1:
            raise SteamPowerParameterError(f"appid must be >= 1, got {appids}")

        appreviews_lst: list[dict[str, Any]] = []

        logger.info("Steam appreviews: reading %s appids, batch_size=%s", len(appids), batch_size)

        for position, appid in enumerate(appids):
            if position > 0 and delay_seconds:
                time.sleep(delay_seconds)

            result = self._get(
                self.REVIEWS_PATH + str(appid),
                {
                    "json": 1,
                    "cc": self.COUNTRY,
                    "l": self.LANGUAGE,
                    "num_per_page": self.REVIEWS_PER_PAGE,
                },
            )

            appreviews_lst.append(self._build_appreviews_row(appid, result))

            if len(appreviews_lst) >= batch_size:
                yield appreviews_lst
                appreviews_lst = []

        if appreviews_lst:
            yield appreviews_lst

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

                match = self.APPTAG_RE.search(page)
                tags = json.loads(match.group(1)) if match else [{}]
            except (SteamPowerConnectionError, json.JSONDecodeError):
                logger.warning("Steam app tag: no tags for appid=%s", appid)
                tags = [{}]

            apptag_lst.extend(self._build_apptag_rows(appid, tags))
            updated_appids.append(appid)
            if len(updated_appids) >= batch_size:
                yield apptag_lst
                apptag_lst = []
                updated_appids = []

        if apptag_lst:
            yield apptag_lst
