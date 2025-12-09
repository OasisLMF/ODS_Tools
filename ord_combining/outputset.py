import json
from ord_combining.common import Analysis, OutputSet


def load_analysis_and_outputsets(ord_output_dirs):
    analysis_set = []
    output_sets = []
    analysis_id = 1
    for ord_dir in ord_output_dirs:
        analysis, _outputsets = parse_analysis_settings(ord_dir / 'analysis_settings.json')

        # set the uid for the analysis
        analysis.id = analysis_id
        for i in range(len(_outputsets)):
            _outputsets[i].analysis_id = analysis_id
        analysis_id += 1

        analysis_set.append(analysis)
        output_sets += _outputsets

    return analysis_set, output_sets


def parse_analysis_settings(settings_fp):
    '''
    Parse an analysis settings file and extract necessary details for OutputSet and AnalysisSet
    '''
    # load settings file
    with open(settings_fp, 'r') as f:
        a_settings = json.load(f)

    # parse the Analysis details
    analysis = Analysis()
    analysis.run_id = a_settings.get('analysis_tag')
    analysis.settings = a_settings
    analysis.path = str(settings_fp.parent)

    # parse the output sets
    perspectives = ['gul', 'il', 'ri']
    outputsets = []

    for p in perspectives:
        if a_settings.get(f'{p}_output', False):
            outputsets += parse_analysis_output_sets(p, analysis)

    return analysis, outputsets


def parse_analysis_output_sets(perspective, analysis):
    '''Parse the output sets from the analysis'''

    summaries = analysis.settings.get(f'{perspective}_summaries', {})
    outputset_list = []
    for summary_level in summaries:
        outputset = OutputSet()
        outputset.perspective_code = perspective
        outputset.exposure_summary_level_id = summary_level['id']
        outputset.exposure_summary_level_fields = summary_level.get('oed_fields', [])
        outputset.analysis_id = analysis.id

        outputset_list.append(outputset)

    return outputset_list
