import logging
import re
import time
from collections.abc import Iterator
from typing import Any

import requests
from airflow.models import Variable
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

logger = logging.getLogger(__name__)


class SteamPowerConnectionError(Exception):
    """Raised on a Steam API/network failure (bad response, timeout, refused connection)."""


class SteamPowerParameterError(Exception):
    """Raised on invalid parameters for Steam API calls."""


class SteamPowerClient:

    SEARCH_PATH = "/search/results/"
    APPID_RE = re.compile(r"/apps/(\d+)/")
    COUNTRY = ""
    LANGUAGE = "en"

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

    def steampower_get_total_count(self, category1: int = 998) -> int:
        """Get the number of results for a given sort_by and category1."""
        payload = self._get(self.SEARCH_PATH, {"infinite": 1, "category1": category1})

        total_count = payload.get("total_count")
        if not isinstance(total_count, int) or total_count < 0:
            raise SteamPowerConnectionError(
                f"Steam search returned no usable total_count: {total_count!r}"
            )
        return total_count

    def steampower_get_search(
        self,
        start: int,
        count: int,
        sort_by: str,
        category1: int = 998,
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
            },
        )
        items = payload.get("items") or []
        if not len(items):
            logger.warning("Steam search: empty page at offset=%s, walking on", start)
        return self._parse_search_items(items)

    def steampower_iter_search(
        self, delay_seconds: float, count: int, sort_by: str, max_rows: int | None = None
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
            total_count = self.steampower_get_total_count()
            offsets = range(0, total_count, count)
            logger.info("Steam search: total_count=%s, reading %s pages", total_count, len(offsets))

        for offset in offsets:
            if offset > 0 and delay_seconds:
                time.sleep(delay_seconds)

            rows = self.steampower_get_search(start=offset, count=count, sort_by=sort_by)
            yield rows
