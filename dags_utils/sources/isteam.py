import logging
import time
from collections.abc import Iterator
from typing import Any

import requests
from airflow.sdk import Variable
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

logger = logging.getLogger(__name__)


class ISteamConnectionError(Exception):
    """Raised on a Steam API/network failure (bad response, timeout, refused connection)."""


class ISteamParameterError(Exception):
    """Raised on invalid parameters for Steam API calls."""


class ISteamClient:
    PLAYERS_PATH = "/ISteamUserStats/GetNumberOfCurrentPlayers/v1/"

    def __init__(self, timeout: int) -> None:
        self._base_url = Variable.get("isteam_base_url").rstrip("/")
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
            raise ISteamConnectionError(f"Steam request failed: {url} params={params}") from exc

    def isteam_get_players(
        self,
        delay_seconds: float,
        appids: list[int],
        batch_size: int = 500,
    ) -> Iterator[list[dict[str, Any]]]:
        """Read ISteamUserStats/GetNumberOfCurrentPlayers"""
        if not appids:
            raise ISteamParameterError("appids must not be empty")
        if min(appids) < 1:
            raise ISteamParameterError(f"appid must be >= 1, got {appids}")

        players_lst: list[dict[str, Any]] = []

        logger.info("Steam active users: reading %s appids, batch_size=%s", len(appids), batch_size)

        for position, appid in enumerate(appids):
            if position > 0 and delay_seconds:
                time.sleep(delay_seconds)
            try:
                result = self._get(self.PLAYERS_PATH, {"appid": appid})
                players_lst.append(
                    {"appid": appid, "player_count": result["response"].get("player_count")}
                )
            except ISteamConnectionError:
                logger.warning("Steam ISteamUserStats: giving up on appid=%s", appid)
                players_lst.append({"appid": appid, "player_count": None})

            if len(players_lst) >= batch_size:
                yield players_lst
                players_lst = []

        if players_lst:
            yield players_lst
