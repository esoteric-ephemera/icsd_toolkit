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
from icsd_toolkit.client.utils import IcsdAdvancedSearchKeys, IcsdSubset, IcsdDataFields, IcsdPropertyDoc

SETTINGS = IcsdClientSettings()
_ICSD_TOKEN_TIMEOUT = 3600 # ICSD tokens expire in one hour

class IcsdClient(BaseModel):
    """Query data via the ICSD API."""

    username : str = Field(SETTINGS.USERNAME)
    password : str = Field(SETTINGS.PASSWORD)
    
    max_retries : float | None = Field(SETTINGS.MAX_RETRIES)
    timeout : float | None = Field(SETTINGS.TIMEOUT)
    max_batch_size : float | None = Field(SETTINGS.MAX_BATCH_SIZE)

    use_document_model : bool = Field(True)

    _auth_token : str | None = PrivateAttr(None)
    _alive_time : float | None = PrivateAttr(None)

    @property
    def _is_windows(self) -> bool:
        return os.name == "nt"
    
    def refresh_session(self) -> None:
        if self._alive_time is None:
            self._alive_time = time()
        
        if (
            self._auth_token is None
            or time() - self._alive_time > 0.98*_ICSD_TOKEN_TIMEOUT
        ):
            self._alive_time = time()
            self.__enter__()
    
    def __enter__(self) -> None:

        response = requests.post(
            "https://icsd.fiz-karlsruhe.de/ws/auth/login",
            headers = {
                "accept": "text/plain",
                "Content-Type": "application/x-www-form-urlencoded"
            },
            data = {
                "loginid": self.username,
                "password": self.password,
            }
        )

        if response.status_code == 200:
            self._auth_token = response.headers["ICSD-Auth-Token"]

    def __exit__(self, *args) -> None:

        _ = requests.get(
            "https://icsd.fiz-karlsruhe.de/ws/auth/logout",
            headers = {
                "accept": "text/plain",
                "ICSD-Auth-Token": self._auth_token,
            }
        )

    def __del__(self) -> None:
        self.__exit__()

    def _search(
        self,
        indices : list[int],
        properties : list[str | IcsdDataFields] | None = None
    ) -> list:
        
        search_props = [
            IcsdDataFields(prop).name for prop in (
                properties or list(IcsdDataFields)
            )
        ]

        if len(indices) > self.max_batch_size:
            batched_ids = np.array_split(indices, np.ceil(len(indices)/self.max_batch_size))

            data = []
            for i, batch in enumerate(batched_ids):
                data.extend(self._search(batch, properties=properties))
            return data
        
        self.refresh_session()
        response = requests.get(
            "https://icsd.fiz-karlsruhe.de/ws/csv",
            headers = {
                "accept": "application/csv",
                "ICSD-Auth-Token": self._auth_token
            },
            params = (
                ("idnum", tuple(indices)),
                ("windowsclient",self._is_windows),
                ("listSelection", properties),
            )
        )

        data = []
        if response.status_code == 200:
            _data = [
                row.split("\t") for row in response.content.decode().splitlines()
            ]
            columns = _data[0][:-1]

            data += [
                {IcsdDataFields[k].value : row[i] for i, k in enumerate(columns)}
                for row in _data[1:]
            ]
            if self.use_document_model:
                data = [
                    IcsdPropertyDoc(**props) for props in data
                ]
            
        return data

    def search(
        self,
        subset : IcsdSubset | str | None = None,
        **kwargs
    ) -> list:
        
        query_vars = []
        for k in IcsdAdvancedSearchKeys:
            if (
                v := kwargs.get(k.value) is not None
            ):
                if isinstance(v, tuple):
                    v = "-".join(v)
                elif isinstance(v, list):
                    v = ",".join(v)
                query_vars.append(f"{k} : {v}")
        query_str = " and ".join(query_vars)

        self.refresh_session()
        response = requests.get(
            "https://icsd.fiz-karlsruhe.de/ws/search/expert",
            headers={
                "accept": "application/xml",
                "ICSD-Auth-Token": self._auth_token,
            },
            params = (
                ("query", query_str),
                ("content type", IcsdSubset(subset) if subset else None)
            ),
        )

        idxs = []
        if (matches := re.match(".*<idnums>(.*)</idnums>.*",response.content.decode())):
            idxs.extend(
                list(matches.groups())[0].split(",")
            )
        
        return self._search(idxs, properties = properties)