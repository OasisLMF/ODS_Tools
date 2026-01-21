'''
Util methods to interact with IO operations for combine.
'''
from pathlib import Path
import numpy as np
import numba as nb
import logging
from datetime import datetime
import pandas as pd

from ods_tools.combine.common import nb_oasis_int, oasis_float

logger = logging.getLogger(__name__)


DEFAULT_OCC_DTYPE = [('event_id', 'i4'),
                     ('period_no', 'i4'),
                     ('occ_date_id', 'i4')  # granular dtype 'i8'
                     ]


def get_default_output_dir():
    timestamp = datetime.now().strftime("%d%m%y%H%M%S")
    return f"./combine_runs/{timestamp}"


def save_summary_info(groupset_summaryinfo, groupset_info, output_dir):
    '''Save grouped summary info

    Args:
        groupset_summaryinfo (dict): Dict with key as groupset_id and value as summaryinfo to save.
        groupset_info (dict): Groupset info dict.
        output_dir (str | pathlib.Path): path to save summary info.
    '''

    for gs, g_summary_info_df in groupset_summaryinfo.items():
        summary_info_fname = groupset_info[gs]['perspective_code'] + f'_GS{gs}_summary-info.csv'
        save_path = Path(output_dir) / summary_info_fname
        g_summary_info_df.to_csv(save_path, index=False)
        logger.info(f'Saved {summary_info_fname}: {save_path}')


def save_output(full_df, output_dir, output_name, factor_col='groupset_id', float_format='%.6f'):
    for i in full_df[factor_col].unique():
        save_path = output_dir / f'{i}_{output_name}'
        full_df.query(f"{factor_col} == {i}").to_csv(save_path, index=False,
                                                     float_format=float_format)
        logger.info(f'Saved {output_name}: {save_path}')

# occurrence reading functions from oasislmf -> copied to avoid circular imports


@nb.jit(nopython=True, cache=True)
def mv_read(byte_mv, cursor, _dtype, itemsize):
    """
    read a certain dtype from numpy byte view starting at cursor, return the value and the index of the end of the object
    Args:
        byte_mv: numpy byte view
        cursor: index of where the object start
        _dtype: data type of the object
        itemsize: size of the data type

    Returns:
        (object value, end of object index)
    """
    return byte_mv[cursor:cursor + itemsize].view(_dtype)[0], cursor + itemsize


def read_occurrence_bin(occ_path, record_dtype=None):
    """Read the occurrence binary file and returns an occurrence map
    Args:
        occ_path (str | os.PathLike): Path to occurrence bin
        record_dtype (list(tuple(str))) : definition of dtypes for each record in occurrence.bin
    Returns:
        occ_map (nb.typed.Dict): numpy map of event_id, period_no, occ_date_id from the occurrence file
    """
    record_dtype = DEFAULT_OCC_DTYPE if record_dtype is None else record_dtype
    record_size = np.sum([np.dtype(el[1]).itemsize for el in record_dtype])

    fin = np.memmap(occ_path, mode="r", dtype="u1")
    cursor = np.dtype(np.int32).itemsize  # skip occ_date
    valid_buff = len(fin)

    if valid_buff - cursor < np.dtype(np.int32).itemsize:
        raise RuntimeError("Error: broken occurrence file, not enough data")

    # Extract no_of_periods
    no_of_periods, cursor = mv_read(fin, cursor, np.int32, np.dtype(np.int32).itemsize)

    num_records = (valid_buff - cursor) // record_size
    if (valid_buff - cursor) % record_size != 0:
        logger.warning(
            f"Occurrence File size (num_records: {num_records}) does not align with expected record size (record_size: {record_size})"
        )

    record_dtype = np.dtype(record_dtype)
    occ_arr = np.zeros(0, dtype=record_dtype)

    if num_records > 0:
        occ_arr = np.frombuffer(fin[cursor:cursor + num_records * record_size], dtype=record_dtype)

    return occ_arr, no_of_periods


def read_occurrence(occ_path, record_dtype=None):
    """Read the occurrence binary file and returns an occurrence map
    Args:
        occ_path (str | os.PathLike): occurrence binary path
        record_dtype (list(str)): definition of dtypes for each record in occurrence.bin
    Returns:
        occ_map (nb.typed.Dict): numpy map of event_id, period_no, occ_date_id from the occurrence file
        no_of_periods (int) : total number of periods
    """
    if record_dtype is None:
        record_dtype = DEFAULT_OCC_DTYPE
    occ_arr, no_of_periods = read_occurrence_bin(occ_path, record_dtype)

    record_dtype = np.dtype(record_dtype)
    occ_map_valtype = record_dtype[["period_no", "occ_date_id"]]
    NB_occ_map_valtype = nb.types.Array(nb.from_dtype(occ_map_valtype), 1, "C")

    occ_map = _read_occ_arr(occ_arr, occ_map_valtype, NB_occ_map_valtype)

    return occ_map, no_of_periods


@nb.njit(cache=True, error_model="numpy")
def _read_occ_arr(occ_arr, occ_map_valtype, NB_occ_map_valtype):
    """Reads occurrence file array and returns an occurrence map of event_id to list of (period_no, occ_date_id)
    """
    occ_map = nb.typed.Dict.empty(nb_oasis_int, NB_occ_map_valtype)
    occ_map_sizes = nb.typed.Dict.empty(nb_oasis_int, nb.types.int64)
    for row in occ_arr:
        event_id = row["event_id"]
        if event_id not in occ_map:
            occ_map[event_id] = np.zeros(8, dtype=occ_map_valtype)
            occ_map_sizes[event_id] = 0
        array = occ_map[event_id]
        current_size = occ_map_sizes[event_id]

        if current_size >= len(array):  # Resize if the array is full
            new_array = np.empty(len(array) * 2, dtype=occ_map_valtype)
            new_array[:len(array)] = array
            array = new_array

        occ_map_current_size = occ_map_sizes[event_id]
        array[occ_map_current_size]["period_no"] = row["period_no"]
        array[occ_map_current_size]["occ_date_id"] = row["occ_date_id"]
        occ_map[event_id] = array
        occ_map_sizes[event_id] += 1

    for event_id in occ_map:
        occ_map[event_id] = occ_map[event_id][:occ_map_sizes[event_id]]

    return occ_map


def load_loss_table_paths(analysis, summary_level_id, perspective, output_type):
    '''Load loss table paths to of type `output_type` from ord output directory of the selected
    analysis, summary_id and perspective.

    Args
    ----
    analysis (Analysis): Analysis info object
    summary_level_id (int): Summary level to load.
    perspective (str): Either `gul`, `il`, or `ri`
    output_type (str): Either period loss table (`plt`), event loss table
                       (`elt`), or `lt` for both.
    '''

    analysis_dir = Path(analysis.path) / 'output'
    glob_str = f'*{perspective}*S{summary_level_id}*{output_type}.csv'
    elt_path_dict = list(analysis_dir.glob(glob_str))
    elt_path_dict = {path.stem.split('_')[-1]: path for path in elt_path_dict}

    return elt_path_dict


# Loading ELT files
MELT_DTYPE = {
    "SummaryId": "i4",
    "SampleType": "i4",
    "EventId": "i4",
    "MeanLoss": oasis_float,
    "SDLoss": oasis_float,
    "MaxLoss": oasis_float
}

QELT_DTYPE = {
    "SummaryId": "i4",
    "EventId": "i4",
    "Quantile": "f4",
    "Loss": "f4"
}

SELT_DTYPE = {
    "SummaryId": "i4",
    "EventId": "i4",
    "SampleId": "i4",
    "Loss": oasis_float
}


def load_elt(path, dtype):
    df = pd.read_csv(path, dtype=dtype)
    return df[dtype.keys()]


def load_melt(path):
    return load_elt(path, MELT_DTYPE)


def load_qelt(path):
    df = load_elt(path, QELT_DTYPE)
    return df.rename(columns={"Loss": "QuantileLoss",
                              "Quantile": "LTQuantile"})


def load_selt(path):
    df = load_elt(path, SELT_DTYPE)
    # Remove negative sampleids
    return df.query('SampleId > 0').rename(columns={"Loss": "SampleLoss"}).reset_index(drop=True)
