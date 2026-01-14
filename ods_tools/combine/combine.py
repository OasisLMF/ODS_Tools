from pathlib import Path
from jsonschema import ValidationError, validate
import json
import logging

logger = logging.getLogger(__name__)

from ods_tools.combine.grouping import ResultGroup
from ods_tools.combine.io import get_default_output_dir
from ods_tools.combine.result import load_analysis_dirs
from ods_tools.combine.sampling import do_loss_sampling, generate_group_periods, generate_gpqt
from ods_tools.oed.common import OdsException

SCHEMA_PATH = Path(Path(__file__).parent / 'config_schema.json')

DEFAULT_CONFIG = {
    "group_fill_perspective": False,
    "group_event_set_fields": [
        "event_set_id",
        "event_set_description",
        "event_occurrence_id",
        "event_occurrence_description",
        "event_occurrence_max_periods",
        "model_supplier_id",
        "model_name_id",
        "model_description",
        "model_version"
    ],
    "group_period_seed": 2479,
    "group_mean": False,
    "group_secondary_uncertainty": False
}


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


def combine(config_file):
    config = read_config(config_file)

    analyses = config.get("analysis_dirs", None)
    if analyses is None:
        logger.error('No `analysis_dirs` set in config.')
        raise OdsException("ORD analyses could not be loaded.")

    output_dir = config.get("output_dir", None)

    if output_dir is None:
        output_dir = get_default_output_dir()

    output_dir = Path(output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)
    logger.info(f'Output directory generated: {output_dir}')

    # Group meta data
    logger.info("Running: Group Step")
    analyses = load_analysis_dirs(analyses)
    group = ResultGroup(analyses, output_dir=output_dir, **config)
    group.prepare_group_info()

    # Period sampling
    logger.info("Running: Period Sampling")
    group_period = generate_group_periods(group,
                                          max_group_periods=config['group_number_of_periods'],
                                          occ_dtype=config.get('occ_dtype', None)
                                          )

    # Loss sampling
    logger.info("Running: Quantile Sampling")
    no_quantile_sampling = config.get('group_mean', False) and not config.get('group_secondary_uncertainty', False)
    gpqt = generate_gpqt(group_period, group,
                         no_quantile_sampling=no_quantile_sampling,
                         correlation=config.get("correlation", None)
                         )

    logger.info("Running: Loss Sampling")
    gplt = do_loss_sampling(gpqt, group,
                            mean_only=config.get("group_mean", False),
                            secondary_uncertainty=config.get("group_secondary_uncertainty", False),
                            parametric_distribution=config.get("group_parametric_distribution"),
                            format_priority=config.get("group_format_priority")
                            )

    # Output generation
    logger.info("Running: Output Generation")

    return gplt


if __name__ == "__main__":
    config_path = Path(Path(__file__).parent / 'config.json')
    output = combine(config_path)
    breakpoint()
