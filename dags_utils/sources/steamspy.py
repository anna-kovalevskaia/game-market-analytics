import logging
import time
from collections.abc import Iterator
from typing import Any

import requests

# from airflow.models import Variable

logger = logging.getLogger(__name__)


class SteamSpyConnectionError(Exception):
    """Raised on a SteamSpy API/network failure (bad response, timeout, etc.)."""


class SteamSpyParameterError(Exception):
    """Raised on invalid parameters for SteamSpy API calls."""


class SteamSpyClient:
    """HTTP-клиент SteamSpy API.

    Docs: https://steamspy.com/api.php
    Airflow Variable: steamspy_base_url.
    """

    def __init__(self, timeout: int) -> None:
        self._base_url = ("https://steamspy.com/api.php").rstrip("/")
        self._timeout = timeout
        self._session = requests.Session()

    def _get(self, params: dict[str, Any]) -> Any:
        logger.info("SteamSpy GET %s params=%s", self._base_url, params)
        try:
            response = self._session.get(self._base_url, params=params, timeout=self._timeout)
            response.raise_for_status()
        except requests.RequestException as exc:
            raise SteamSpyConnectionError(f"SteamSpy request failed: params={params}") from exc
        return response.json()

    def steamspy_get_all(self, page: int) -> dict[str, Any]:
        """Одна страница SteamSpy ?request=all&page=N. Возвращает {appid_str: {info}}."""
        if page < 0:
            raise SteamSpyParameterError(f"page must be >= 0, got {page}")
        return self._get({"request": "all", "page": page})

    def steamspy_iter_all(
        self,
        max_pages: int,
        stop_after_empty_pages: int,
        delay_seconds: int,
    ) -> Iterator[tuple[str, dict[str, Any]]]:
        if max_pages < 1:
            raise SteamSpyParameterError(f"max_pages must be >= 1, got {max_pages}")
        if stop_after_empty_pages < 1:
            raise SteamSpyParameterError(
                f"stop_after_empty_pages must be >= 1, got {stop_after_empty_pages}"
            )
        if delay_seconds < 0:
            raise SteamSpyParameterError(f"delay_seconds must be >= 0, got {delay_seconds}")

        empty_in_a_row = 0
        for page in range(max_pages):
            if page > 0 and delay_seconds:
                time.sleep(delay_seconds)

            data = self.steamspy_get_all(page=page)

            if not data:
                empty_in_a_row += 1
                logger.info(
                    "SteamSpy page=%s empty (%s/%s consecutive)",
                    page,
                    empty_in_a_row,
                    stop_after_empty_pages,
                )
                if empty_in_a_row >= stop_after_empty_pages:
                    logger.info(
                        "SteamSpy: stop after %s consecutive empty pages", empty_in_a_row
                    )
                    return
                continue

            empty_in_a_row = 0
            logger.info("SteamSpy page=%s ok, keys=%s", page, len(data))
            yield str(page), data
        else:
            logger.warning(
                "SteamSpy: reached max_pages=%s without %s consecutive empty pages - "
                "catalog may be larger than max_pages, data likely truncated",
                max_pages,
                stop_after_empty_pages,
            )

    def steamspy_get_appdetails(self, appid: int) -> dict[str, Any]:
        """SteamSpy ?request=appdetails&appid=N."""
        if appid <= 0:
            raise SteamSpyParameterError(f"appid must be > 0, got {appid}")
        return self._get({"request": "appdetails", "appid": appid})

    # ---------- request=top100* / genre / tag ----------
    #   steamspy_get_top100in2weeks() -> {"request": "top100in2weeks"} -- maybe unnecessary
    #   steamspy_get_top100forever()  -> {"request": "top100forever"} -- maybe unnecessary
    #   steamspy_get_top100owned()    -> {"request": "top100owned"} -- maybe unnecessary
    #   steamspy_get_genre(genre)     -> {"request": "genre", "genre": genre}
    #                                  -- not needed, can get from steamspy_get_appdetails
    #   steamspy_get_tag(tag)         -> {"request": "tag", "tag": tag}
    #                                  -- not needed, can get from steamspy_get_appdetails