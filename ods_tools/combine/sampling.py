from ods_tools.combine.common import DEFAULT_RANDOM_SEED
from ods_tools.combine.io import read_occurrence_bin
import pandas as pd
import numpy as np

rng = np.random.default_rng(DEFAULT_RANDOM_SEED)  # todo implement configurable random seed


def generate_group_periods(group, max_group_periods,
                           occ_dtype=None):
    '''
    Performs period sampling for a specified `ResultGroup` with the set `max_group_periods`.

    Args:
        group (ResultGroup) : Group of ORD to generate group periods from.
        max_group_periods (int) : Maximum number of group periods.
        occ_dtype ( List[Tuple[str]] ) : dtype of records in occurrence files.

    Returns:
        group_periods (pd.DataFrame): The sampled group periods.
    '''
    group_period_fragments = []

    for groupeventset_id, curr_groupeventset in group.groupeventset.items():
        _analysis_ids = curr_groupeventset['analysis_ids']
        occurence_files = [group.analyses[a].path / 'input/occurrence.bin' for a in _analysis_ids]

        periods, max_periods = load_period_info(occurence_files, occ_dtype)
        curr_frag = gen_group_periods_event_set_analysis(periods,
                                                         max_period=max_periods,
                                                         max_group_periods=max_group_periods)
        curr_frag = pd.concat(curr_frag)
        curr_frag['groupeventset_id'] = groupeventset_id

        group_period_fragments.append(curr_frag)

    group_period = pd.concat(group_period_fragments)
    return group_period.sort_values(by=['groupeventset_id', 'GroupPeriod']).reset_index(drop=True)


def load_period_info(occ_paths, occ_dtype=None):
    '''
    Read period data for multiple analyses from the `occurrence.bin` files.

    Args:
        occ_paths (list[str]) : list of occurrence files to load periods from.
    '''
    periods = []
    max_periods = None

    for occ_path in occ_paths:
        occ_arr, _max_periods = read_occurrence_bin(occ_path, occ_dtype)
        periods += np.unique(occ_arr['period_no']).tolist()

        if max_periods is None:
            max_periods = _max_periods
        elif max_periods != _max_periods:
            raise Exception('Currently does not support different max_periods in a group.')

    return list(set(periods)), max_periods


def gen_group_periods_event_set_analysis(periods, max_period, max_group_periods):
    # generate the group cycle slices
    group_cycles = max_group_periods // max_period
    group_slices = [(i * max_period, (i + 1) * max_period) for i in range(group_cycles)]

    if group_cycles * max_period < max_group_periods:
        group_slices += [(group_cycles * max_period, max_group_periods)]

    group_period_fragments = []
    for slice in group_slices:
        shuffled_slice = np.arange(slice[0] + 1, slice[1] + 1)
        rng.shuffle(shuffled_slice)
        shuffle_filter = min(len(shuffled_slice), len(periods))

        group_period_fragments.append(pd.DataFrame({'GroupPeriod': shuffled_slice[:shuffle_filter],
                                                    'Period': periods[:shuffle_filter]}))
    return group_period_fragments
