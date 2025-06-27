"""Retrieve CIF and metadata from the ICSD API.

This module is based on
    https://github.com/lrcfmd/ICSDClient/
"""

from __future__ import annotations

import multiprocessing.queues
import os
import re
import requests
from time import time

import logging

import multiprocessing
import numpy as np
from pydantic import BaseModel, Field, PrivateAttr

from icsd_toolkit.client.settings import IcsdClientSettings
from icsd_toolkit.client.enums import (
    IcsdAdvancedSearchKeys,
    IcsdSubset,
    IcsdDataFields,
)
from icsd_toolkit.client.schemas import IcsdPropertyDoc

SETTINGS = IcsdClientSettings()
_ICSD_TOKEN_TIMEOUT = 3600  # ICSD tokens expire in one hour

logger = logging.getLogger("icsd_toolkit.client")


class IcsdClient(BaseModel):
    """Query data via the ICSD API."""

    username: str = Field(SETTINGS.USERNAME)
    password: str = Field(SETTINGS.PASSWORD)

    max_retries: float | None = Field(SETTINGS.MAX_RETRIES)
    timeout: float | None = Field(SETTINGS.TIMEOUT)
    max_batch_size: float | None = Field(SETTINGS.MAX_BATCH_SIZE)

    use_document_model: bool = Field(True)
    num_parallel_requests: int | None = Field(None)

    _auth_token: str | None = PrivateAttr(None)
    _session_start_time: float | None = PrivateAttr(None)

    @property
    def _is_windows(self) -> bool:
        return os.name == "nt"

    def refresh_session(self, force: bool = False) -> None:
        if self._session_start_time is None:
            self._session_start_time = time()

        if (
            self._auth_token is None
            or ((time() - self._session_start_time) > 0.98 * _ICSD_TOKEN_TIMEOUT)
            or force
        ):
            self.logout()
            self._session_start_time = time()
            self.login()

    def login(self) -> None:
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
            if self._auth_token is None:
                logger.info(response.content)
        else:
            logger.info(response.content)

    def logout(self) -> None:
        _ = self._get(
            "https://icsd.fiz-karlsruhe.de/ws/auth/logout",
            headers={
                "accept": "text/plain",
            },
        )
        self._auth_token = None
        self._session_start_time = None

    def __enter__(self) -> None:
        self.login()
        return self

    def __exit__(self, *args) -> None:
        self.logout()

    def __del__(self) -> None:
        self.logout()

    def _get(self, *args, **kwargs) -> requests.Response:
        self.refresh_session()

        headers = kwargs.pop("headers", {})
        headers["ICSD-Auth-Token"] = self._auth_token

        params: tuple[str] = kwargs.pop("params", ())
        params = tuple(list(params) + [("windowsclient", self._is_windows)])

        resp = requests.get(*args, **kwargs, headers=headers, params=params)
        if resp.status_code != 200:
            logger.info(resp.content)
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
        include_metadata: bool = True,
        _data: list | None = None,
    ) -> list:

        self.refresh_session(force=True)
        search_props = [
            (
                IcsdDataFields[prop].name
                if prop in IcsdDataFields.__members__
                else IcsdDataFields(prop).name
            )
            for prop in (properties or list(IcsdDataFields))
        ]

        if len(indices) > self.max_batch_size:
            batched_ids: list[list[str]] = [
                v.tolist()
                for v in np.array_split(
                    indices, np.ceil(len(indices) / self.max_batch_size)
                )
            ]

            data = []
            for i, batch in enumerate(batched_ids):
                data.extend(
                    self._search(
                        batch,
                        properties=search_props,
                        include_cif=include_cif,
                        include_metadata=include_metadata,
                        _data=_data,
                    )
                )
            return data

        if not include_cif and not include_metadata:
            return list(indices)

        if include_metadata:
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
                csv_data = [
                    row.split("\t") for row in response.content.decode().splitlines()
                ]
                columns = csv_data[0][:-1]

                data += [
                    {IcsdDataFields[k].value: row[i] for i, k in enumerate(columns)}
                    for row in _data[1:]
                ]
            else:
                logger.info(response.content)

        if include_cif:
            cifs = self._get_cifs(indices)
            if include_metadata:
                for i, doc in enumerate(data):
                    data[i]["cif"] = cifs.get(int(doc["collection_code"]))
            else:
                data = [{"collection_code": cc, "cif": cif} for cc, cif in cifs.items()]

        if self.use_document_model:
            data = [IcsdPropertyDoc(**props) for props in data]

        if _data:
            _data.extend(data)
        return data

    def search(
        self,
        subset: IcsdSubset | str | None = None,
        properties: list[str | IcsdDataFields] | None = None,
        include_cif: bool = False,
        include_metadata: bool = True,
        **kwargs,
    ) -> list:

        query_vars = []
        for k in IcsdAdvancedSearchKeys:
            if (v := kwargs.get(k.value)) is not None:
                if isinstance(v, tuple):
                    v = f"{v[0]}-{v[1]}"
                elif isinstance(v, list):
                    v = ",".join(v)
                query_vars.append(f"{k.name.lower()} : {v}")
        query_str = " and ".join(query_vars)

        params = [("query", query_str)]
        if subset:
            params.append(("content type", IcsdSubset(subset).name))

        response = self._get(
            "https://icsd.fiz-karlsruhe.de/ws/search/expert",
            headers={
                "accept": "application/xml",
            },
            params=params,
        )

        idxs: list[str] = []
        if matches := re.match(".*<idnums>(.*)</idnums>.*", response.content.decode()):
            idxs.extend(list(matches.groups())[0].split())

        if self.num_parallel_requests and len(idxs) > self.num_parallel_requests:
            batched_idxs = np.array_split(idxs, self.num_parallel_requests)

            manager = multiprocessing.Manager()
            procs = []
            res = manager.list()
            for iproc in range(self.num_parallel_requests):
                proc = multiprocessing.Process(
                    target=self._search,
                    args=(batched_idxs[iproc].tolist(),),
                    kwargs={
                        "properties": properties,
                        "include_cif": include_cif,
                        "include_metadata": include_metadata,
                        "_data": res,
                    },
                )
                proc.start()
                procs.append(proc)

            for proc in procs:
                proc.join()
            return list(res)

        return self._search(
            idxs,
            properties=properties,
            include_cif=include_cif,
            include_metadata=include_metadata,
        )
