import json
import logging
import re
from datetime import datetime
from typing import Any

logger = logging.getLogger(__name__)


class SteamPowerResultParser:
    APPID_RE = re.compile(r"/apps/(\d+)/")
    APPTAG_RE = re.compile(r"InitAppTagModal\(\s*\d+\s*,\s*(\[.*?\])\s*,", re.S)
    DATE_FORMATS = ("%b %d, %Y", "%d %b, %Y", "%d %B, %Y", "%B %d, %Y")

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
    def parse_search_items(cls, items: list[dict[str, Any]]) -> list[dict[str, Any]]:
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
    def parse_apptag_page(cls, appid: int, page: str) -> list[dict[str, Any]]:
        """Pull the tag array out of the store page — age-gated pages carry none."""
        match = cls.APPTAG_RE.search(page)
        if match is None:
            logger.warning("Steam app tag: no tag block on the page for appid=%s", appid)
            return [{}]
        return json.loads(match.group(1))

    @classmethod
    def build_appdetails_row(cls, appid: int, raw: dict[str, Any]) -> dict[str, Any]:
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
            "dlc": data.get("dlc", []),
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
    def build_price_row(cls, appid: int, data: dict[str, Any]) -> dict[str, Any]:
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
    def build_packages_rows(cls, appid: int, data: dict[str, Any]) -> list[dict[str, Any]]:
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
    def build_appreviews_row(cls, appid: int, data: dict[str, Any]) -> dict[str, Any]:
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
    def build_appreviews_details_rows(
        cls, appid: int, data: dict[str, Any]
    ) -> list[dict[str, Any]]:
        """One raw.steampower_appreviews_details row per review on the /appreviews/ page."""
        rows = []
        for review in data.get("reviews") or []:
            # Playtime lives in the author block, everything else at the top level.
            author = review.get("author") or {}
            rows.append(
                {
                    "appid": appid,
                    "timestamp_created": review.get("timestamp_created", None),
                    "timestamp_updated": review.get("timestamp_updated", None),
                    "voted_up": review.get("voted_up", None),
                    "language": review.get("language", None),
                    "steam_purchase": review.get("steam_purchase", None),
                    "received_for_free": review.get("received_for_free", None),
                    "written_during_early_access": review.get("written_during_early_access", None),
                    "refunded": review.get("refunded", None),
                    "primarily_steam_deck": review.get("primarily_steam_deck", None),
                    "playtime_at_review": author.get("playtime_at_review", None),
                    "playtime_forever": author.get("playtime_forever", None),
                    "playtime_last_two_weeks": author.get("playtime_last_two_weeks", None),
                    "last_played": author.get("last_played", None),
                }
            )
        return rows

    @classmethod
    def build_dlc_details_row(cls, dlc_appid: int, data: dict[str, Any]) -> dict[str, Any]:
        """One raw.steampower_dlc_details row out of an /api/appdetails/ entry of an add-on."""
        platforms = data.get("platforms") or {}
        return {
            "appid": data["fullgame"]["appid"],
            "dlc_appid": dlc_appid,
            "dlc_name": data.get("name", ""),
            "type": data.get("type", ""),
            "is_free": data.get("is_free", None),
            "release_date": cls._try_date_parse((data.get("release_date") or {}).get("date", "")),
            "platform_windows": platforms.get("windows", None),
            "platform_mac": platforms.get("mac", None),
            "platform_linux": platforms.get("linux", None),
        }

    @classmethod
    def build_dlc_price_row(cls, dlc_appid: int, data: dict[str, Any]) -> dict[str, Any]:
        """One raw.steampower_dlc_price row. price_overview is absent for free add-ons."""
        price = data.get("price_overview") or {}
        return {
            "appid": data["fullgame"]["appid"],
            "dlc_appid": dlc_appid,
            "currency": price.get("currency", ""),
            "price_initial": price.get("initial", None),
            "price_final": price.get("final", None),
            "discount_percent": price.get("discount_percent", None),
        }

    @classmethod
    def build_dlc_packages_rows(cls, dlc_appid: int, data: dict[str, Any]) -> list[dict[str, Any]]:
        """Flatten package_groups[].subs[] into raw.steampower_dlc_packages rows."""
        return [
            {
                "appid": data["fullgame"]["appid"],
                "dlc_appid": dlc_appid,
                "packageid": pckg_data.get("packageid"),
                "package_option_text": pckg_data.get("option_text", ""),
                "package_price_with_discount": pckg_data.get("price_in_cents_with_discount", None),
            }
            for group in data.get("package_groups") or []
            for pckg_data in group.get("subs") or []
        ]

    @classmethod
    def build_apptag_rows(cls, appid: int, data: list[dict[str, Any]]) -> list[dict[str, Any]]:
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
