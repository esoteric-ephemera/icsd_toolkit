from __future__ import annotations

from contextlib import redirect_stderr, redirect_stdout
from io import StringIO
import json
from typing import TYPE_CHECKING
import warnings

from pathlib import Path
from pydantic import BaseModel, field_serializer, ConfigDict

from pymatgen.core import Composition, Structure
from pymatgen.io.cif import CifParser

from icsd_toolkit.client.enums import IcsdSubset
from icsd_toolkit.client.schemas import IcsdPropertyDoc

try:
    from pycodcif import parse as cod_cif_parse

except ImportError:
    cod_cif_parse = None

if TYPE_CHECKING:
    from typing_extensions import Self

# These are only used for the pycodcif -> pymatgen interface
if cod_cif_parse:
    from tempfile import NamedTemporaryFile
    from pymatgen.io.cif import CifBlock
    from string import printable

    ASCII_CHARS = set(printable)


def get_chemsys_from_structure(structure: Structure):
    return sorted(
        set([element for element in structure.composition.remove_charges().as_dict()])
    )


def _pycodcif_to_pymatgen(
    cif_str: str, cif_parser: CifParser | None = None
) -> list[Structure]:

    structures = []
    if cod_cif_parse is None:
        raise ImportError("Please pip install `pycodcif` to use this feature.")

    temp_file = NamedTemporaryFile(suffix=".cif")
    with open(temp_file.name, "w", encoding="utf-8") as f:
        # remove non-ASCII characters
        f.write("".join(filter(lambda x: x in ASCII_CHARS, cif_str)))
        f.seek(0)

    meta, num_errors, errors = cod_cif_parse(temp_file.name,{"fix_all": 1})
    if num_errors:
        warnings.warn("pycodcif:\n" + "\n  ".join(errors))

    cif_parser = cif_parser or CifParser(temp_file.name)

    for block in meta:
        for k in ("data", "values"):
            if (data := block.get(k, None)) is not None:
                break
        cif_block = CifBlock(data, *[block.get(k) for k in ("loops", "name")])
        structures.append(
            cif_parser._get_structure(cif_block, primitive=True, symmetrized=False)
        )

    temp_file.close()

    return structures


class IcsdStructureDoc(BaseModel):

    model_config = ConfigDict(use_enum_values=True)

    chemsys: str | None = None
    ions: list[str] | None = None
    nelements: int | None = None
    volume_per_atom: float | None = None
    density: float | None = None
    num_sites: int | None = None
    space_group_number: int | None = None
    space_group_symbol: str | None = None
    is_ordered: bool | None = None
    path: Path | None = None
    icsd_id: int | None = None
    remarks: list[str] | None = None
    subset: IcsdSubset | None = None
    matched_icsd_ids: list[str] | None = None

    structure: Structure | None = None
    composition: dict[str, float] | None = None
    cif: str | None = None

    @field_serializer("structure")
    def sanitize_structure_for_parquet(self, structure: Structure | None) -> str | None:
        if structure is not None:
            return json.dumps(structure.as_dict())
        return None

    @classmethod
    def from_cif_str(cls, cif_str: str, **kwargs) -> Self:

        config = {
            "remarks": [],
            **kwargs,
        }
        with StringIO(initial_value=cif_str) as stream:
            parser = CifParser(stream)

        parse_fail = False
        try:
            with redirect_stderr(StringIO()), redirect_stdout(StringIO()):
                structure = parser.parse_structures(primitive=True)[0]

        except Exception as exc_pmg:

            config["remarks"].append(str(exc_pmg))
            if cod_cif_parse:
                try:

                    structure = _pycodcif_to_pymatgen(cif_str, cif_parser=parser)[0]
                except Exception as exc_pycodcif:
                    config["remarks"].append(str(exc_pycodcif))
                    parse_fail = True
            else:
                parse_fail = True

        if parse_fail:
            config["cif"] = cif_str
            return cls(**config)

        composition = Composition(structure.composition.as_dict())

        # Some ICSD structures are disordered only in manually assigned oxidation states
        if struct_is_disordered := not structure.is_ordered:

            try:
                if (
                    Structure(
                        structure.lattice,
                        species=[site.species for site in structure],
                        coords=structure.frac_coords,
                        coords_are_cartesian=False,
                        charge=structure.charge,
                    )
                    .remove_oxidation_states()
                    .is_ordered
                ):
                    struct_is_disordered = False

            except ValueError:
                pass

        config.update(
            {
                "structure": structure,
                "ions": [str(ele) for ele in structure.composition],
                "chemsys": "-".join(get_chemsys_from_structure(structure)),
                "num_elements": len(composition.elements),
                "volume_per_atom": structure.volume / structure.num_sites,
                "density": structure.density,
                "is_ordered": not struct_is_disordered,
            }
        )

        with redirect_stderr(StringIO()), redirect_stdout(StringIO()):
            remarks = parser.check(structure)
        if remarks:
            config["remarks"].append(remarks)

        try:
            config["space_group_symbol"], config["space_group_number"] = (
                structure.get_space_group_info()
            )
        except Exception as exc:
            config["remarks"].append(str(exc))

        if len(config["remarks"]) == 0:
            config["remarks"] = None

        return cls(**config)

    @classmethod
    def from_icsd_property_doc(cls, doc: IcsdPropertyDoc) -> Self:
        if doc.cif is None:
            return cls(icsd_id=doc.collection_code)
        return cls.from_cif_str(
            doc.cif,
            icsd_id=doc.collection_code,
            subset = doc.subset,
        )
