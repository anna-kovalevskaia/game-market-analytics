import logging
import re
import time
from collections.abc import Iterator
from typing import Any

import requests
from airflow.models import Variable

logger = logging.getLogger(__name__)


class SteamPowerConnectionError(Exception):
    """Raised on a Steam API/network failure (bad response, timeout, refused connection)."""


class SteamPowerParameterError(Exception):
    """Raised on invalid parameters for Steam API calls."""


class SteamPowerClient:
    """HTTP client for the Steam storefront (store.steampowered.com).

    The store endpoints are undocumented, but they return JSON and need no API key.
    Airflow Variable: steam_store_base_url.
    """

    SEARCH_PATH = "/search/results/"
    # appid appears only inside the image URL: .../steam/apps/<appid>/capsule_sm_120.jpg
    APPID_RE = re.compile(r"/apps/(\d+)/")

    def __init__(self, timeout: int) -> None:
        self._base_url = Variable.get("steam_store_base_url").rstrip("/")
        self._timeout = timeout
        self._session = requests.Session()

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
        return rows

    def steampower_get_search(
        self, start: int, count: int, sort_by: str, category1: int
    ) -> list[dict[str, Any]]:
        """One /search/results/ page. Returns [{"appid": int, "name": str}, ...].

        The server may return a different number of rows than requested, so callers
        must page by what came back rather than by `count`.
        """
        if start < 0:
            raise SteamPowerParameterError(f"start must be >= 0, got {start}")
        if count < 1:
            raise SteamPowerParameterError(f"count must be >= 1, got {count}")

        payload = self._get(
            self.SEARCH_PATH,
            {
                "query": "",
                "start": start,
                "count": count,
                "sort_by": sort_by,
                "category1": category1,
                "json": 1,
            },
        )
        return self._parse_search_items(payload.get("items") or [])

    def steampower_iter_search(
        self,
        max_pages: int,
        delay_seconds: float,
        start: int,
        count: int,
        sort_by: str,
        category1: int,
    ) -> Iterator[tuple[int, list[dict[str, Any]]]]:
        if max_pages < 1:
            raise SteamPowerParameterError(f"max_pages must be >= 1, got {max_pages}")
        if delay_seconds < 0:
            raise SteamPowerParameterError(f"delay_seconds must be >= 0, got {delay_seconds}")

        offset = start
        for page in range(max_pages):
            if page > 0 and delay_seconds:
                time.sleep(delay_seconds)

            rows = self.steampower_get_search(
                start=offset, count=count, sort_by=sort_by, category1=category1
            )

            if not rows:
                logger.info("Steam search: empty page at offset=%s, stopping", offset)
                return

            logger.info("Steam search offset=%s ok, rows=%s", offset, len(rows))
            yield offset, rows

            offset += len(rows)
        else:
            logger.warning(
                "Steam search: reached max_pages=%s without an empty page - "
                "data likely truncated at offset=%s",
                max_pages,
                offset,
            )
