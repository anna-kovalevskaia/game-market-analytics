import logging
from typing import Any

import requests
from airflow.models import Variable

logger = logging.getLogger(__name__)


class SteamSpyError(Exception):
    """SteamSpy API error."""


class SteamSpyClient:
    """HTTP-клиент SteamSpy API.

    Docs: https://steamspy.com/api.php
    Airflow Variable: steamspy_base_url (обязательная, без default — fail-fast).
    """

    def __init__(self, base_url: str | None = None, timeout: int = 30) -> None:
        self._base_url = (base_url or Variable.get("steamspy_base_url")).rstrip("/")
        self._timeout = timeout
        self._session = requests.Session()

    def _get(self, params: dict[str, Any]) -> Any:
        logger.info("SteamSpy GET %s params=%s", self._base_url, params)
        response = self._session.get(self._base_url, params=params, timeout=self._timeout)
        response.raise_for_status()
        return response.json()

    # ---------- request=all ----------

    def steamspy_get_all(self, page: int = 0) -> dict[str, Any]:
        """SteamSpy ?request=all&page=N. Возвращает {appid_str: {info}}."""
        if page < 0:
            raise SteamSpyError(f"page must be >= 0, got {page}")
        return self._get({"request": "all", "page": page})

    # ---------- request=appdetails ----------

    def steamspy_get_appdetails(self, appid: int) -> dict[str, Any]:
        """SteamSpy ?request=appdetails&appid=N."""
        if appid <= 0:
            raise SteamSpyError(f"appid must be > 0, got {appid}")
        return self._get({"request": "appdetails", "appid": appid})

    # ---------- request=top100* / genre / tag ----------
    #   steamspy_get_top100in2weeks()    -> {"request": "top100in2weeks"}
    #   steamspy_get_top100forever()     -> {"request": "top100forever"}
    #   steamspy_get_top100owned()       -> {"request": "top100owned"}
    #   steamspy_get_genre(genre)        -> {"request": "genre", "genre": genre}
    #   steamspy_get_tag(tag)            -> {"request": "tag", "tag": tag}