from __future__ import annotations

from contextlib import redirect_stderr, redirect_stdout
from io import StringIO
import json
from string import printable
from tempfile import NamedTemporaryFile
from typing import TYPE_CHECKING

from pathlib import Path
from pydantic import BaseModel, field_serializer, model_validator, ConfigDict

from pymatgen.core import Composition, Structure
from pymatgen.io.cif import CifParser

from xtalxd.icsd.client.client.enums import IcsdSubset
from xtalxd.icsd.client.client.schemas import IcsdPropertyDoc

from pycodcif.pycodcif import CifFile, cif_print

if TYPE_CHECKING:
    from typing import Any
    from typing_extensions import Self

ASCII_CHARS = set(printable)

DEFAULT_COD_CIF_OPTIONS = {
    k: 1
    for k in (
        "fix_errors",
        "fix_data_header",
        "fix_datablock_names",
        "fix_duplicate_tags_with_same_values",
        "fix_duplicate_tags_with_empty_values",
        "fix_string_quotes",
        "allow_uqstring_brackets",
        "fix_ctrl_z",
        "fix_non_ascii_symbols",
        "fix_missing_closing_double_quote",
        "fix_missing_closing_single_quote",
    )
}


def _pycodcif_to_pymatgen_from_file(
    file_name: str | Path,
) -> list[Structure]:

    cif_obj = CifFile(file_name, DEFAULT_COD_CIF_OPTIONS)

    stdout = StringIO()
    stderr = StringIO()
    with redirect_stdout(stdout), redirect_stderr(stderr):
        cif_print(cif_obj._cif)
        stdout_text = stdout.read()
        stderr_text = stderr.read()

    return stdout_text, stderr_text, cif_obj


def _pycodcif_to_pymatgen_from_str(
    cif_str: str,
) -> list[Structure]:
    temp_file = NamedTemporaryFile(suffix=".cif")
    with open(temp_file.name, "w", encoding="utf-8") as f:
        # remove non-ASCII characters
        f.write(cif_str)
        f.seek(0)

    output = _pycodcif_to_pymatgen_from_file(temp_file.name)
    temp_file.close()
    return output


def get_chemsys_from_structure(structure: Structure):
    return sorted(
        set([element for element in structure.composition.remove_charges().as_dict()])
    )


# def _pycodcif_to_pymatgen_from_file(
#     file_name : str | Path,
# ) -> list[Structure]:

#     cif_obj = CifFile(file_name)
#     with StringIO(initial_value=str(cif_obj)) as stio:
#         structures = CifParser(stio).parse_structures(primitive=True)

#     if num_errors:
#         warnings.warn("pycodcif:\n" + "\n  ".join(errors))

#     cif_parser = cif_parser or CifParser(file_name)

#     for block in meta:
#         for k in ("data", "values"):
#             if (data := block.get(k, None)) is not None:
#                 break

#         try:
#             cif_block = CifBlock(data, *[block.get(k) for k in ("loops", "name")])
#             structures.append(
#                 cif_parser._get_structure(cif_block, primitive=True, symmetrized=False)
#             )
#         except Exception:
#             continue
#     return structures


def _pycodcif_to_pymatgen(
    cif_str: str, cif_parser: CifParser | None = None
) -> list[Structure]:

    temp_file = NamedTemporaryFile(suffix=".cif")
    with open(temp_file.name, "w", encoding="utf-8") as f:
        # remove non-ASCII characters
        f.write("".join(filter(lambda x: x in ASCII_CHARS, cif_str)))
        f.seek(0)

    structures = _pycodcif_to_pymatgen_from_file(temp_file.name, cif_parser=cif_parser)

    temp_file.close()

    return structures


class IcsdStructureDoc(BaseModel):

    model_config = ConfigDict(use_enum_values=True)

    chemsys: str | None = None
    ions: list[str] | None = None
    num_elements: int | None = None
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
    matched_icsd_ids: list[int] | None = None

    structure: Structure | None = None
    composition: dict[str, float] | None = None
    cif: str | None = None

    @field_serializer("structure", "composition")
    def sanitize_structure_for_parquet(
        self, field: Structure | Composition | None
    ) -> str | None:
        if field is not None:
            if hasattr(field, "as_dict"):
                field = field.as_dict()
            return json.dumps(field)
        return None

    @model_validator(mode="before")
    @classmethod
    def from_dct_obj(cls, config: Any) -> Any:
        for k in ("structure", "composition"):
            if isinstance(config.get(k), str):
                config[k] = json.loads(config[k])
        return config

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
            try:

                structure = _pycodcif_to_pymatgen(cif_str, cif_parser=parser)[0]
            except Exception as exc_pycodcif:
                config["remarks"].append(str(exc_pycodcif))
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
                "num_elements": len(composition.elements),
                "composition": composition.as_dict(),
                "num_sites": len(structure),
                "volume_per_atom": structure.volume / len(structure),
                "is_ordered": not struct_is_disordered,
            }
        )

        try:
            config["chemsys"] = "-".join(get_chemsys_from_structure(structure))
            config["density"] = structure.density
        except Exception as exc:
            config["remarks"].append(f"chemsys: {exc}")

        with redirect_stderr(StringIO()), redirect_stdout(StringIO()):
            try:
                remarks = parser.check(structure)
            except Exception as exc:
                remarks = f"pmg_check: {exc}"
        if remarks:
            config["remarks"].append(remarks)

        try:
            config["space_group_symbol"], config["space_group_number"] = (
                structure.get_space_group_info()
            )
        except Exception as exc:
            config["remarks"].append(f"spacegroup: {exc}")

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
            subset=doc.subset,
        )
