import pandas as pd
from ord_combining.common import list_col_to_string

def create_group_set_df(outputsets_df, group_id):
    group_set_cols = ['perspective_code', 'exposure_summary_level_fields_string']
    group_set_df = outputsets_df.drop_duplicates(subset=group_set_cols)

    group_set_df = group_set_df.reset_index(drop=True)
    group_set_df['group_id'] = group_id
    group_set_df['id'] = group_set_df.index

    return group_set_df


def merge_group_set_output_set(group_set_df, outputsets_df):
    group_set_cols = ['perspective_code', 'exposure_summary_level_fields_string']
    # create merged group output set mapping
    merged_df = pd.merge(outputsets_df, group_set_df,
                how='inner', on=group_set_cols, suffixes=("_output_set", "_group_set"))
    merged_df = merged_df.rename(columns={"id_output_set": "output_set_id",
                                        "id_group_set": "group_set_id"})
    merged_df['id'] = merged_df.index

    return merged_df

def generate_group_analysis(outputsets_df, group_id=1):
    # convert to categorical col
    outputsets_df['exposure_summary_level_fields_string'] = list_col_to_string(outputsets_df['exposure_summary_level_fields'])

    group_set_df = create_group_set_df(outputsets_df, group_id)

    merged_df = merge_group_set_output_set(group_set_df, outputsets_df)

    # extract group analysis
    group_analysis_df = merged_df[['group_id', 'analysis_id_output_set']]
    group_analysis_df = group_analysis_df.reset_index(drop=True)
    group_analysis_df['id'] = group_analysis_df.index
    group_analysis_df = group_analysis_df.rename(columns={"analysis_id_output_set": "analysis_id"})

    return group_analysis_df

def parse_field_from_analysis_settings(field, settings):
    field_location_map = {
            "event_set_id" : ["model_settings", "event_set"],
            "event_occurrence_id" : ["model_settings", "event_occurrence_id"],
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


def generate_group_event_set(analysis, group_analysis_df, group_event_set_fields):

    # generate event occurrence set
    event_occurrence_set = []
    for a in analysis.values():
        extracted_analysis = extract_group_event_set_fields(a, group_event_set_fields)
        extracted_analysis['analysis_id'] = a.id

        event_occurrence_set.append(extracted_analysis)


    full_event_occurrence_set_df = pd.DataFrame(event_occurrence_set)

    event_occurrence_set_df= full_event_occurrence_set_df.drop_duplicates(subset=group_event_set_fields)

    event_occurrence_set_df = event_occurrence_set_df.reset_index(drop=True)
    event_occurrence_set_df['id'] = event_occurrence_set_df.index + 1
    event_occurrence_set_df = event_occurrence_set_df[['id'] + group_event_set_fields]

    group_event_set_analysis = pd.merge(full_event_occurrence_set_df, event_occurrence_set_df, on=group_event_set_fields,
                                        how='left').rename(columns={'id': 'group_event_set_id'})
    group_event_set_analysis = group_event_set_analysis[['analysis_id', 'group_event_set_id']]


    return event_occurrence_set_df, group_event_set_analysis

