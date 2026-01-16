from pathlib import Path
from jsonschema import ValidationError, validate
import json
import logging

from ods_tools.combine.grouping import ResultGroup, create_combine_group
from ods_tools.combine.io import get_default_output_dir, save_output, save_summary_info
from ods_tools.combine.output_generation import generate_alt, generate_ept
from ods_tools.combine.result import load_analysis_dirs
from ods_tools.combine.sampling import do_loss_sampling, generate_group_periods, generate_gpqt
from ods_tools.combine.common import DEFAULT_CONFIG
from ods_tools.oed.common import OdsException

logger = logging.getLogger(__name__)
SCHEMA_PATH = Path(Path(__file__).parent / 'config_schema.json')


def read_config(config_path):
    """
    Read config and add defaults where necessary. Performs validation against
    config schema.
    """
    with open(config_path, 'r') as f:
        config = json.load(f)
    config = DEFAULT_CONFIG | config

    with open(SCHEMA_PATH, 'r') as f:
        schema = json.load(f)

    try:
        validate(config, schema)
    except ValidationError as e:
        raise OdsException(f'Config Validation error: {e.message}')

    return config


def combine(analysis_dirs,
            group_event_set_fields,
            group_number_of_periods,
            group_mean=False,
            group_secondary_uncertainty=False,
            group_parametric_distribution='beta',
            group_format_priority=['M', 'Q', 'S'],
            group_correlation=None,
            occ_dtype=None,
            group_plt=False,
            group_alt=False,
            group_ept=False,
            group_ept_oep=True,
            group_ept_aep=True,
            output_dir=None,
            **kwargs
            ):
    # prepare output dir
    if output_dir is None:
        output_dir = get_default_output_dir()

    output_dir = Path(output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)
    logger.info(f'Output directory generated: {output_dir}')

    # Group meta data
    logger.info("Running: Group Step")
    analyses = load_analysis_dirs(analysis_dirs)
    group, groupset_summaryinfo = create_combine_group(analyses,
                                                       groupeventset_fields=group_event_set_fields)

    save_summary_info(groupset_summaryinfo, group.groupset, output_dir)

    # Period sampling
    logger.info("Running: Period Sampling")
    group_period = generate_group_periods(group,
                                          max_group_periods=group_number_of_periods,
                                          occ_dtype=occ_dtype
                                          )

    # Loss sampling
    logger.info("Running: Quantile Sampling")
    no_quantile_sampling = group_mean and not group_secondary_uncertainty
    gpqt = generate_gpqt(group_period, group,
                         no_quantile_sampling=no_quantile_sampling,
                         correlation=group_correlation
                         )

    logger.info("Running: Loss Sampling")
    gplt = do_loss_sampling(gpqt, group,
                            mean_only=group_mean,
                            secondary_uncertainty=group_secondary_uncertainty,
                            parametric_distribution=group_parametric_distribution,
                            format_priority=group_format_priority
                            )

    # Output generation
    logger.info("Running: Output Generation")

    outputs = []

    if group_plt:
        outputs.append(('plt', gplt))

    if group_alt:
        outputs.append(('alt', generate_alt(gplt, group_number_of_periods)))

    if group_ept:
        outputs.append(('ept',
                        generate_ept(gplt, group_number_of_periods,
                                     oep=group_ept_oep,
                                     aep=group_ept_aep)))

    for output_name, output_df in outputs:
        save_output(output_df, output_dir, f'{output_name}.csv')

    return gplt


if __name__ == "__main__":
    config_path = Path(Path(__file__).parent / 'config.json')
    config = read_config(config_path)
    output = combine(**config)
