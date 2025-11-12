from __future__ import annotations

from pathlib import Path
from typing import TYPE_CHECKING
import json

import multiprocessing
import pandas as pd

from pymatgen.analysis.structure_matcher import StructureMatcher, ElementComparator

from xtalxd.analysis.schemas import IcsdStructureDoc

if TYPE_CHECKING:
    from pymatgen.core import Structure


def aggregate_logs(cache_dir: str | Path) -> dict[str, list[list[int]]]:
    processed_chemsys = {}
    for p in Path(cache_dir).glob("*jsonl"):
        with open(p, "r") as f:
            for line in f:
                processed_chemsys.update(json.loads(line))
    return processed_chemsys


class StructureGrouper:

    __slots__ = ("structure_matcher", "max_structure_size", "nproc", "cache_dir")

    def __init__(
        self,
        structure_matcher: StructureMatcher | None = None,
        max_structure_size: int | None = 100,
        nproc: int = 1,
        cache_dir: str | Path | None = None,
    ) -> None:

        self.structure_matcher = structure_matcher or StructureMatcher(
            primitive_cell=True,
            attempt_supercell=True,
            comparator=ElementComparator(),
        )

        self.max_structure_size = max_structure_size
        self.nproc = nproc
        self.cache_dir = None
        if cache_dir:
            self.cache_dir = Path(cache_dir)
            if not self.cache_dir.exists():
                self.cache_dir.mkdir(exist_ok=True, parents=True)

    def _group_structures(
        self,
        structures: dict[str, list[Structure]],
        structure_groups: list[int],
        cache_file: str | Path | None = None,
    ) -> None:

        for cs, struct_in_cs in structures.items():
            groups = self.structure_matcher.group_structures(struct_in_cs)
            icsd_groups = [
                sorted([structure._icsd_id for structure in group]) for group in groups
            ]

            structure_groups.extend(icsd_groups)

            if cache_file:
                with open(cache_file, "a") as f:
                    f.write(json.dumps({cs: icsd_groups}) + "\n")

    def group_structures(
        self,
        structure_docs: list[IcsdStructureDoc],
    ) -> list[IcsdStructureDoc]:

        chemsys = set([doc.chemsys for doc in structure_docs])
        by_chemsys = {cs: [] for cs in chemsys}

        unique_idx: list[int] = []
        for doc in structure_docs:
            if self.max_structure_size and doc.num_sites > self.max_structure_size:
                unique_idx.append([doc.icsd_id])
                continue

            struct = doc.structure
            struct._icsd_id = doc.icsd_id
            by_chemsys[doc.chemsys].append(struct)

        for cs in chemsys:
            if len(by_chemsys[cs]) == 1:
                struct = by_chemsys.pop(cs)[0]
                unique_idx.append([struct._icsd_id])

        sorted_chemsys = sorted(by_chemsys, key=lambda k: len(by_chemsys[k]))
        process_targets = [{} for _ in range(self.nproc)]
        for iproc, cs in enumerate(sorted_chemsys):
            process_targets[iproc % self.nproc][cs] = by_chemsys[cs]

        manager = multiprocessing.Manager()
        grouped_ids = manager.list(unique_idx)

        procs = []
        for iproc in range(self.nproc):

            proc_kwargs = {}
            if self.cache_dir:
                proc_kwargs["cache_file"] = self.cache_dir / f"{iproc}.jsonl"

            proc = multiprocessing.Process(
                target=self._group_structures,
                args=(
                    process_targets[iproc],
                    grouped_ids,
                ),
                kwargs=proc_kwargs,
            )

            proc.start()
            procs.append(proc)

        for proc in procs:
            proc.join()

        unique_docs = []
        docs_by_icsd_id = {doc.icsd_id: doc for doc in structure_docs}
        for id_group in list(grouped_ids):
            min_idx = id_group[0]
            doc = docs_by_icsd_id[min_idx].model_dump()
            doc["matched_icsd_ids"] = id_group
            unique_docs.append(IcsdStructureDoc(**doc))

        return unique_docs

    def group_structures_from_dataframe(
        self, dataframe: pd.DataFrame, **kwargs
    ) -> list[IcsdStructureDoc]:

        icsd_docs = []
        for idx in dataframe.index:
            try:
                doc = IcsdStructureDoc(**dataframe.loc[idx])
                icsd_docs.append(doc)
            except Exception:
                continue

        return self.group_structures(icsd_docs, **kwargs)
