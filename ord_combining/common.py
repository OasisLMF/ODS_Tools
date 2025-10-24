from dataclasses import dataclass, asdict, field
from typing import List
import pandas as pd

# %% define data classes

@dataclass
class OutputSet():
    '''OutputSetTable field data class'''
    id: int = None
    perspective_code: str = None
    analysis_id: int = None # link to AnalysisTable
    exposure_summary_level_fields: List[str] = field(default_factory=lambda : list)
    exposure_summary_level_id: int = None

@dataclass
class Analysis():
    '''AnalysisTable field data class'''
    id: int = None
    run_id: str = None
    description: str = ''
    settings: dict = field(default_factory=lambda : dict)
    path: str=''

def dataclass_list_to_dataframe(dataclass_list):
    return pd.DataFrame([asdict(c) for c in dataclass_list])
