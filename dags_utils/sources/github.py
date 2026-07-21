import logging

import requests
from airflow.hooks.base import BaseHook
from airflow.sdk import Variable

logger = logging.getLogger(__name__)


class GitHubConnectionError(Exception):
    """Raised on a GitHub API/network failure (bad response, timeout, etc.)."""


class GitHubParameterError(Exception):
    """Raised on invalid parameters for GitHub API calls."""


class GitHubClient:

    def __init__(
        self, repo: str | None = None, timeout: int | None = None, branch: str | None = None
    ) -> None:
        if repo is None or timeout is None:
            cfg = Variable.get("github_config", deserialize_json=True)
            repo = repo or cfg["repo"]
            timeout = timeout or cfg["timeout"]
            branch = branch or cfg["branch"]
        self._repo = repo
        self._timeout = timeout
        self._branch = branch
        self._token = BaseHook.get_connection("github_token").password
        self._base_url = "https://api.github.com"
        self._session = requests.Session()

    def _get_headers(self) -> dict[str, str]:
        return {
            "Authorization": f"Bearer {self._token}",
            "Accept": "application/vnd.github+json",
        }

    def get_latest_commit_sha(self) -> str:

        if not self._branch:
            raise GitHubParameterError("branch must be non-empty")

        url = f"{self._base_url}/repos/{self._repo}/commits/{self._branch}"
        try:
            response = self._session.get(url, headers=self._get_headers(), timeout=self._timeout)
            response.raise_for_status()
        except requests.RequestException as exc:
            raise GitHubConnectionError(f"GitHub request failed: {url}") from exc

        return response.json()["sha"]

    def get_changed_files(self, base_sha: str, head_sha: str) -> list[str]:

        if not base_sha or not head_sha:
            raise GitHubParameterError("base_sha and head_sha must be non-empty")

        url = f"{self._base_url}/repos/{self._repo}/compare/{base_sha}...{head_sha}"
        try:
            response = self._session.get(url, headers=self._get_headers(), timeout=self._timeout)
            response.raise_for_status()
        except requests.RequestException as exc:
            raise GitHubConnectionError(f"GitHub compare failed: {url}") from exc

        data = response.json()
        files = [f["filename"] for f in data.get("files", [])]
        logger.info("GitHub changed files %s", files)
        return files
