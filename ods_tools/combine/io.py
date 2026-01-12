'''
Util methods to interact with IO operations for combine.
'''
from pathlib import Path
import numpy as np
import numba as nb
import logging

from ods_tools.combine.common import nb_oasis_int

logger = logging.getLogger(__name__)

DEFAULT_OCC_DTYPE = [('event_id', 'i4'),
                     ('period_no', 'i4'),
                     ('occ_date_id', 'i4')  # granular dtype 'i8'
                     ]


def save_summary_info(groupset_summaryinfo, groupset_info, output_dir):
    '''Save grouped summary info

    Args:
        groupset_summaryinfo (dict): Dict with key as groupset_id and value as summaryinfo to save.
        groupset_info (dict): Groupset info dict.
        output_dir (str | pathlib.Path): path to save summary info.
    '''

    for gs, g_summary_info_df in groupset_summaryinfo.items():
        summary_info_fname = f'{groupset_info[gs]['perspective_code']}_GS{gs}_summary-info.csv'
        g_summary_info_df.to_csv(Path(output_dir) / summary_info_fname, index=False)


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
