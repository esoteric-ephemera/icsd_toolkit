"""Define enum and other client utilities."""

from enum import Enum
import re

from pydantic import BaseModel, Field, model_validator
from uncertainties import ufloat_fromstr

class IcsdSubset(Enum):
    EXPERIMENTAL_INORGANIC = "experimental_inorganic"
    EXPERIMENTAL_METALORGANIC = "experimental_metalorganic"
    THERORETICAL_STRUCTURES = "theoretical"

class IcsdAdvancedSearchKeys(Enum):

    AUTHORS = "authors"
    ARTICLE = "article"
    PUBLICATIONYEAR = "publication_year"
    PAGEFIRST = "page_first"
    JOURNAL = "journal"
    VOLUME = "volume"
    ABSTRACT = "abstract"
    KEYWORDS = "keywords"
    CELLVOLUME = "cell_volume"
    CALCDENSITY = "calc_density"
    CELLPARAMETERS = "cell_parameters"
    SEARCH = "search"
    STRUCTUREDFORMULA = "structured_formula"
    CHEMICALNAME = "chemical_name"
    MINERALNAME = "mineral_name"
    MINERALGROUP = "mineral_group"
    ZVALUECHEMISTRY = "z_value_chemistry"
    ANXFORMULA = "anx_formula"
    ABFORMULA = "ab_formula"
    FORMULAWEIGHT = "formula_weight"
    NUMBEROFELEMENTS = "number_of_elements"
    COMPOSITION = "composition"
    COLLECTIONCODE = "collection_code"
    PDFNUMBER = "pdf_number"
    RELEASE = "release"
    RECORDINGDATE = "recording_date"
    MODIFICATIONDATE = "modification_date"
    COMMENT = "comment"
    RVALUE = "r_value"
    TEMPERATURE = "temperature"
    PRESSURE = "pressure"
    SAMPLETYPE = "sample_type"
    RADIATIONTYPE = "radiation_type"
    STRUCTURETYPE = "structure_type"
    SPACEGROUPSYMBOL = "space_groups_ymbol"
    SPACEGROUPNUMBER = "space_group_number"
    BRAVAISLATTICE = "bravais_lattice"
    CRYSTALSYSTEM = "crystal_system"
    CRYSTALCLASS = "crystal_class"
    LAUECLASS = "laue_class"
    WYCKOFFSEQUENCE = "wyckoff_sequence"
    PEARSONSYMBOL = "pearson_symbol"
    INVERSIONCENTER = "inversion_center"
    POLARAXIS = "polaraxis"

class IcsdDataFields(Enum):
    
    CollectionCode = "collection_code"
    CcdcNo = "ccdc_no"
    HMS = "h_m_s"
    StructuredFormula = "structured_formula"
    StructureType = "structure_type"
    Title = "title"
    Authors = "authors"
    Reference = "reference"
    CellParameter = "cell_parameter"
    ReducedCellParameter = "reduced_cell_parameter"
    StandardisedCellParameter = "standardised_cell_parameter"
    CellVolume = "cell_volume"
    FormulaUnitsPerCell = "formula_units_per_cell"
    FormulaWeight = "formula_weight"
    Temperature = "temperature"
    Pressure = "pressure"
    RValue = "r_value"
    SumFormula = "sum_formula"
    ANXFormula = "a_n_x_formula"
    ABFormula = "a_b_formula"
    ChemicalName = "chemical_name"
    MineralName = "mineral_name"
    MineralNameIma = "mineral_name_ima"
    MineralGroup = "mineral_group"
    MineralSeries = "mineral_series"
    MineralRootGroup = "mineral_root_group"
    MineralSubGroup = "mineral_sub_group"
    MineralSuperGroup = "mineral_super_group"
    MineralSubClass = "mineral_sub_class"
    MineralClass = "mineral_class"
    CalculatedDensity = "calculated_density"
    MeasuredDensity = "measured_density"
    PearsonSymbol = "pearson_symbol"
    WyckoffSequence = "wyckoff_sequence"
    Journal = "journal"
    Volume = "volume"
    PublicationYear = "publication_year"
    Page = "page"
    Quality = "quality"
    Keywords = "keywords"
    Ccdc = "ccdc"
    Pdf = "pdf"

class CellParameters(BaseModel):

    a : float | None = None
    b : float | None = None
    c : float | None = None    
    alpha : float | None = None
    beta : float | None = None
    gamma : float | None = None
    
    a_uncertainty : float | None = None
    b_uncertainty : float | None = None
    c_uncertainty : float | None = None

    alpha_uncertainty : float | None = None
    beta_uncertainty : float | None = None
    gamma_uncertainty : float | None = None

    @model_validator(mode="before")
    @classmethod
    def from_str(cls, config):
        """Parse space-separated lattice parameters."""
        lps = ["a","b","c","alpha","beta","gamma"]
        if isinstance(config,str):
            vals = config.split()
            config = {}
            for i, lp in enumerate(lps):
                if "(" in vals[i]:
                    v_w_u = ufloat_fromstr(vals[i])
                    config[lp] = v_w_u.n
                    config[f"{lp}_uncertainty"] = v_w_u.s
                else:
                    config[lp] = float(vals[i])        

        return config

class IcsdPropertyDoc(BaseModel):
    """General container for ICSD data."""

    collection_code : int | None = Field(
        None, description="The ICSD identifier of this entry."
    )
    cif : str | None = Field(
        None, description="The CIF file associated with this entry."
    )
    ccdc_no : int | None = Field(None)
    ccdc : int | None = None

    h_m_s : str | None = None
    pearson_symbol : str | None = None
    wyckoff_sequence : str | None = None

    structured_formula : str | None = None
    sum_formula : str | None = None
    a_n_x_formula : str | None = None
    a_b_formula : str | None = None

    structure_type : str | None = None
    title : str | None = None
    authors : list[str] | None = None
    journal : str | None = None
    publication_year : int | None = None
    volume : int | None = None
    page : str | None = None
    reference : str | None = None

    cell_parameter : CellParameters | None = None
    reduced_cell_parameter : CellParameters | None = None
    standardised_cell_parameter: CellParameters | None = None

    cell_volume : float | None = None
    formula_units_per_cell : int | None = None
    formula_weight : float | None = None
    
    temperature : float | None = None
    pressure : float | None = None
    r_value : float | None = None

    chemical_name : str | None = None
    mineral_name : str | None = None
    mineral_name_ima : str | None = None
    mineral_group : str | None = None
    mineral_series : str | None = None
    mineral_root_group : str | None = None
    mineral_sub_group : str | None = None
    mineral_super_group : str | None = None
    mineral_sub_class : str | None = None
    mineral_class : str | None = None

    calculated_density : float | None = None
    measured_density : float | None = None

    quality : int | None = None
    keywords : str | None = None
    
    pdf : str | None = None

    @model_validator(mode="before")
    @classmethod
    def deserialize(cls,config):
        if isinstance(config.get("authors"),str):
            config["authors"] = config["authors"].split(";")
        for k, v in config.items():
            if isinstance(v,str) and len(v) == 0:
                config[k] = None
        return config