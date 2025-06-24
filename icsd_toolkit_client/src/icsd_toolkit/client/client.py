"""Retrieve CIF and metadata from the ICSD API.

This module is based on
    https://github.com/lrcfmd/ICSDClient/
"""

from __future__ import annotations

import os
import re
import requests
from time import time

import numpy as np
from pydantic import BaseModel, Field, PrivateAttr

from icsd_toolkit.client.settings import IcsdClientSettings
from icsd_toolkit.client.utils import (
    IcsdAdvancedSearchKeys,
    IcsdSubset,
    IcsdDataFields,
    IcsdPropertyDoc,
)

SETTINGS = IcsdClientSettings()
_ICSD_TOKEN_TIMEOUT = 3600  # ICSD tokens expire in one hour


class IcsdClient(BaseModel):
    """Query data via the ICSD API."""

    username: str = Field(SETTINGS.USERNAME)
    password: str = Field(SETTINGS.PASSWORD)

    max_retries: float | None = Field(SETTINGS.MAX_RETRIES)
    timeout: float | None = Field(SETTINGS.TIMEOUT)
    max_batch_size: float | None = Field(SETTINGS.MAX_BATCH_SIZE)

    use_document_model: bool = Field(True)

    _auth_token: str | None = PrivateAttr(None)
    _session_start_time: float | None = PrivateAttr(None)

    @property
    def _is_windows(self) -> bool:
        return os.name == "nt"

    def refresh_session(self) -> None:
        if self._session_start_time is None:
            self._session_start_time = time()

        if self._auth_token is None or (
            (time() - self._session_start_time) > 0.98 * _ICSD_TOKEN_TIMEOUT
        ):
            self._session_start_time = time()
            self.__enter__()

    def __enter__(self) -> None:

        response = requests.post(
            "https://icsd.fiz-karlsruhe.de/ws/auth/login",
            headers={
                "accept": "text/plain",
                "Content-Type": "application/x-www-form-urlencoded",
            },
            data={
                "loginid": self.username,
                "password": self.password,
            },
        )
        if response.status_code == 200:
            self._auth_token = response.headers["ICSD-Auth-Token"]

    def __exit__(self, *args) -> None:

        _ = self._get(
            "https://icsd.fiz-karlsruhe.de/ws/auth/logout",
            headers={
                "accept": "text/plain",
            },
        )
        self._auth_token = None
        self._session_start_time = None

    def __del__(self) -> None:
        self.__exit__()

    def _get(self, *args, **kwargs) -> requests.Response:
        self.refresh_session()

        headers = kwargs.pop("headers", {})
        headers["ICSD-Auth-Token"] = self._auth_token

        params: tuple[str] = kwargs.pop("params", ())
        params = tuple(list(params) + [("windowsclient", self._is_windows)])

        resp = requests.get(*args, **kwargs, headers=headers, params=params)
        return resp

    def _get_cifs(self, collection_codes: int | list[int]) -> dict[int, str]:
        if isinstance(collection_codes, int) or len(collection_codes) == 1:
            cif_str = self._get(
                f"https://icsd.fiz-karlsruhe.de/ws/cif/{collection_codes[0]}",
                headers={
                    "accept": "application/cif",
                },
            ).content.decode()
        else:
            cif_str = self._get(
                "https://icsd.fiz-karlsruhe.de/ws/cif/multiple",
                headers={
                    "accept": "application/cif",
                },
                params=[("idnum", collection_codes)],
            ).content.decode()

        return {
            int(re.search(r"_database_code_ICSD ([0-9]+)", cif_body).group(1)): "#(C)"
            + cif_body
            for cif_body in cif_str.split("\n#(C)")[1:]
        }

    def _search(
        self,
        indices: list[int],
        properties: list[str | IcsdDataFields] | None = None,
        include_cif: bool = False,
    ) -> list:

        search_props = [
            (
                IcsdDataFields[prop].name
                if prop in IcsdDataFields.__members__
                else IcsdDataFields(prop).name
            )
            for prop in (properties or list(IcsdDataFields))
        ]

        if len(indices) > self.max_batch_size:
            batched_ids = np.array_split(
                indices, np.ceil(len(indices) / self.max_batch_size)
            )

            data = []
            for i, batch in enumerate(batched_ids):
                data.extend(self._search(batch, properties=search_props))
            return data

        response = self._get(
            "https://icsd.fiz-karlsruhe.de/ws/csv",
            headers={
                "accept": "application/csv",
            },
            params=(
                ("idnum", tuple(indices)),
                ("listSelection", search_props),
            ),
        )

        data = []
        if response.status_code == 200:
            _data = [row.split("\t") for row in response.content.decode().splitlines()]
            columns = _data[0][:-1]

            data += [
                {IcsdDataFields[k].value: row[i] for i, k in enumerate(columns)}
                for row in _data[1:]
            ]

        if include_cif:
            cifs = self._get_cifs(indices)
            for i, doc in enumerate(data):
                data[i]["cif"] = cifs.get(int(doc["collection_code"]))

        if self.use_document_model:
            return [IcsdPropertyDoc(**props) for props in data]
        return data

    def search(
        self,
        subset: IcsdSubset | str | None = None,
        properties: list[str | IcsdDataFields] | None = None,
        include_cif: bool = False,
        **kwargs,
    ) -> list:

        query_vars = []
        for k in IcsdAdvancedSearchKeys:
            if (v := kwargs.get(k.value)) is not None:
                if isinstance(v, tuple):
                    v = "-".join(v)
                elif isinstance(v, list):
                    v = ",".join(v)
                query_vars.append(f"{k.name.lower()} : {v}")
        query_str = " and ".join(query_vars)

        self.refresh_session()
        response = self._get(
            "https://icsd.fiz-karlsruhe.de/ws/search/expert",
            headers={
                "accept": "application/xml",
            },
            params=(
                ("query", query_str),
                ("content type", IcsdSubset(subset).name if subset else None),
            ),
        )

        idxs = []
        if matches := re.match(".*<idnums>(.*)</idnums>.*", response.content.decode()):
            idxs.extend(list(matches.groups())[0].split())

        return self._search(idxs, properties=properties, include_cif=include_cif)
