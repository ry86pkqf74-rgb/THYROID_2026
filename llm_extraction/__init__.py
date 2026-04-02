from llm_extraction.base import BaseExtractor, EntityMatch
from llm_extraction.vocab import NOTE_TYPES, ENTITY_SCHEMA_COLUMNS

from llm_extraction.extract_molecular_v2 import MolecularDetailExtractor
from llm_extraction.extract_rai_v2 import RAIDetailExtractor
from llm_extraction.extract_imaging_v2 import ImagingNoduleExtractor
from llm_extraction.extract_operative_v2 import OperativeDetailExtractor
from llm_extraction.extract_histology_v2 import HistologyDetailExtractor

ALL_V2_EXTRACTORS: list[type[BaseExtractor]] = [
    MolecularDetailExtractor,
    RAIDetailExtractor,
    ImagingNoduleExtractor,
    OperativeDetailExtractor,
    HistologyDetailExtractor,
]

__all__ = [
    "BaseExtractor",
    "EntityMatch",
    "NOTE_TYPES",
    "ENTITY_SCHEMA_COLUMNS",
    "MolecularDetailExtractor",
    "RAIDetailExtractor",
    "ImagingNoduleExtractor",
    "OperativeDetailExtractor",
    "HistologyDetailExtractor",
    "ALL_V2_EXTRACTORS",
]
