"""
Numba jit functions used by OedSchema.peril_filtering

Kept in a separate module so that numba is only imported when peril filtering is used,
numba is an optional dependency (pip install ods-tools[combine])
"""
import numba as nb
import numpy as np

peril_groups_dict_key_type = nb.types.UnicodeCharSeq(3)
peril_groups_dict_value_type = nb.types.UnicodeCharSeq(3)[:]


# function to check if a subperil is part of a peril
@nb.jit(cache=True, nopython=True)
def __single_peril_filtering(peril_id, peril_filter, perils_dict):
    if peril_filter:
        for peril_filter in peril_filter.split(';'):
            for p in perils_dict[peril_filter]:
                if p == peril_id:
                    return True
    return False


@nb.jit(cache=True, nopython=True)
def jit_peril_filtering(peril_ids, peril_filters, perils_dict):
    result = np.empty_like(peril_ids, dtype=np.bool_)
    for i in range(peril_ids.shape[0]):
        result[i] = __single_peril_filtering(peril_ids[i], peril_filters[i], perils_dict)

    return result


@nb.jit(cache=True, nopython=True)
def make_peril_groups_dict(peril_group_keys, peril_group_index, peril_lists):
    nb_peril_groups_dict = nb.typed.Dict.empty(
        key_type=peril_groups_dict_key_type,
        value_type=peril_groups_dict_value_type,
    )
    for i in range(peril_group_keys.shape[0]):
        nb_peril_groups_dict[peril_group_keys[i]] = peril_lists[peril_group_index[i]: peril_group_index[i + 1]]

    return nb_peril_groups_dict


def make_perils_dict(perils_covered):
    """numba dict peril group to sub_peril"""
    nb_perils_dict = nb.typed.Dict.empty(
        key_type=peril_groups_dict_key_type,
        value_type=peril_groups_dict_value_type,
    )
    for peril_group, perils in perils_covered.items():
        nb_perils_dict[peril_group] = np.array(perils, dtype='U3')

    return nb_perils_dict
