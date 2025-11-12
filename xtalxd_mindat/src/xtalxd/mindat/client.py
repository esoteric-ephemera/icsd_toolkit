"""Tools for querying the mindat api, see https://www.mindat.org/ for details."""

from functools import partial, cached_property
import multiprocessing
from pydantic import BaseModel, Field, PrivateAttr
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from urllib.parse import urljoin
from typing import Any


from xtalxd.mindat.settings import MindatClientSettings

SETTINGS = MindatClientSettings()


class MindatClient(BaseModel):

    api_key: str = Field(SETTINGS.API_KEY)
    max_retries: int = Field(SETTINGS.MAX_RETRIES)
    _url: str = PrivateAttr(SETTINGS.API_ENDPOINT)
    _session: requests.Session | None = PrivateAttr(None)

    @property
    def headers(self) -> dict[str, str]:
        return {"Authorization": f"Token {self.api_key}"}

    @property
    def session(self) -> requests.Session:
        if self._session is None:
            self._session = self._get_session()
        return self._session

    def __enter__(self):
        """Support for "with" context."""
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """Support for "with" context."""
        self.session.close()

    def _get_session(self) -> requests.Session:
        session = requests.Session()
        session.headers = {"Authorization": f"Token {self.api_key}"}
        retry = Retry(
            total=self.max_retries,
            read=self.max_retries,
            connect=self.max_retries,
            respect_retry_after_header=True,
            status_forcelist=[429, 504, 502],  # rate limiting
            backoff_factor=0.1,
        )
        adapter = HTTPAdapter(max_retries=retry)
        session.mount("http://", adapter)
        session.mount("https://", adapter)
        return session

    def _get(self, url):
        return self.session.get(url, params={"format": "json"}).json()

    def get_mindat_endpoints(self):
        """Get a list of possible endpoints to search through.

        An endpoint is basically a different collection of data.
        """
        return self._get(urljoin(self._url, "v1"))

    @cached_property
    def _valid_endpoints(self) -> set[str]:
        return set(self.get_mindat_endpoints())

    def _get_fields_from_response(
        self,
        url: str,
        fields: list[str] | None = None,
    ) -> tuple[str, list[dict[str, Any]]] | tuple[None, list]:
        next_url = None
        data = []
        for itry in range(self.max_retries):
            try:
                decoded = self._get(url)
                if fields:
                    data += [
                        {k: entry.get(k) for k in fields if entry.get(k)}
                        for entry in decoded["results"]
                    ]
                else:
                    data += decoded["results"]
                next_url = decoded["next"]
                break

            except Exception as exc:
                print(
                    f"Query failed with exception (try {itry}/{self.max_retries}):\n{exc}"
                )
        return next_url, data

    def get_mindat_data_by_endpoint(
        self,
        endpoint: str,
        paginate: bool = False,
        fields: list[str] | None = None,
        parallel_requests: int = 1,
    ):
        """

        Get the data available in an endpoint.

        paginate is used to retrive all results (True) or just the first
        set (False)

        If `fields` is a non-empty list, only those fields will be returned.
        """

        # if endpoint not in self._valid_endpoints:
        #     raise ValueError(
        #         f"Unknown {endpoint=}, valid endpoints are {', '.join(self._valid_endpoints)}"
        #     )

        next_url = f"{self._url}/v1/{endpoint}"
        if parallel_requests > 1 and paginate:
            urls = {next_url}
            while next_url:
                try:
                    decoded = self._get(next_url)
                    next_url = decoded.get("next")
                    if next_url:
                        urls.add(next_url)
                except Exception:
                    continue

            _get = partial(self._get_fields_from_response, fields=fields)
            with multiprocessing.Pool(parallel_requests) as pool:
                _outputs = pool.map(_get, list(urls))
            return [entry for outputs in _outputs for entry in outputs[1]]

        data = []
        while next_url:
            next_url, new_data = self._get_fields_from_response(next_url, fields=fields)
            data.extend(new_data)
            if not paginate:
                next_url = None
        return data
