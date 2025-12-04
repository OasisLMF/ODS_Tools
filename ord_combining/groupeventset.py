import pandas as pd
from ord_combining.common import list_col_to_string


def create_group_set_df(outputsets_df, group_id):
    group_set_cols = ['perspective_code', 'exposure_summary_level_fields_string']

    group_df = outputsets_df.copy().rename(columns={'id': 'output_set_id'})
    group_df['group_id'] = group_id
    group_df['group_set_id'] = outputsets_df.groupby(group_set_cols).ngroup()

    output_cols = ['group_set_id', 'group_id'] + group_set_cols
    group_set_df = group_df[output_cols].drop_duplicates(subset='group_set_id').sort_values('group__set_id')

    group_output_set_df = group_df[['group_set_id', 'output_set_id']]
    return group_set_df, group_output_set_df


def generate_group_set(outputsets_df, group_id=1):
    # convert to categorical col
    outputsets_df['exposure_summary_level_fields_string'] = list_col_to_string(outputsets_df['exposure_summary_level_fields'])

    # intermediary table
    group_set_cols = ['perspective_code', 'exposure_summary_level_fields_string']
    group_df = outputsets_df.copy().rename(columns={'id': 'output_set_id'})
    group_df['group_id'] = group_id
    group_df['group_set_id'] = outputsets_df.groupby(group_set_cols).ngroup()

    group_set_df = (group_df[['group_set_id', 'group_id'] + group_set_cols]
                    .drop_duplicates(subset='group_set_id')
                    .sort_values('group_set_id')
                    .set_index('group_set_id'))
    group_output_set = group_df[['group_set_id', 'output_set_id']].set_index("output_set_id").to_dict()['group_set_id']

    return group_set_df, group_output_set


def parse_field_from_analysis_settings(field, settings):
    field_location_map = {
        "event_set_id": ["model_settings", "event_set"],
        "event_occurrence_id": ["model_settings", "event_occurrence_id"],
    }

    field_path = field_location_map.get(field, [field])
    value = settings

    for key in field_path:
        value = value.get(key, {})

    if value:
        return value
    return None


def extract_group_event_set_fields(analysis, group_event_set_fields):
    output = {}
    for field in group_event_set_fields:
        output[field] = parse_field_from_analysis_settings(field, analysis.settings)

    return output


def generate_group_event_set(analysis, group_event_set_fields):

    # generate event occurrence set
    event_occurrence_set = []
    for a in analysis.values():
        extracted_analysis = extract_group_event_set_fields(a, group_event_set_fields)
        extracted_analysis['analysis_id'] = a.id

        event_occurrence_set.append(extracted_analysis)

    full_event_occurrence_set_df = pd.DataFrame(event_occurrence_set)

    event_occurrence_set_df = full_event_occurrence_set_df.drop_duplicates(subset=group_event_set_fields)

    event_occurrence_set_df = event_occurrence_set_df.reset_index(drop=True)
    event_occurrence_set_df['event_occurrence_set_id'] = event_occurrence_set_df.index + 1
    event_occurrence_set_df = event_occurrence_set_df[['event_occurrence_set_id'] + group_event_set_fields]
    print('Full event occurrence set:\n', full_event_occurrence_set_df)
    print('Event occurrence set:\n', event_occurrence_set_df)

    event_occurrence_set_analysis = pd.merge(full_event_occurrence_set_df,
                                             event_occurrence_set_df,
                                             on=group_event_set_fields,
                                             how='left')
    event_occurrence_set_analysis = event_occurrence_set_analysis[['analysis_id', 'event_occurrence_set_id']]

    return event_occurrence_set_df, event_occurrence_set_analysis
