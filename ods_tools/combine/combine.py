from pathlib import Path
import jsonschema
import json
import logging

from ods_tools.combine.grouping import create_combine_group
from ods_tools.combine.io import get_default_output_dir, save_output, save_summary_info
from ods_tools.combine.output_generation import generate_alt, generate_ept
from ods_tools.combine.result import load_analysis_dirs
from ods_tools.combine.sampling import do_loss_sampling, generate_group_periods, generate_gpqt
from ods_tools.combine.common import DEFAULT_CONFIG, GALT_schema, GEPT_schema, GPLT_headers, GPLT_schema
from ods_tools.oed.common import OdsException

logger = logging.getLogger(__name__)
SCHEMA_PATH = Path(Path(__file__).parent.parent / 'data' / 'combine_settings_schema.json')


class CombineSettingsSchema:
    def __init__(self, schema_path=None):
        if schema_path is None:
            schema_path = SCHEMA_PATH

        with open(schema_path, 'r') as f:
            self.schema = json.load(f)

    def validate(self, config, raise_error=True):
        validator = jsonschema.Draft4Validator(self.schema)
        validation_errors = [e for e in validator.iter_errors(config)]

        is_valid = validator.is_valid(config)

        exception_msgs = {}
        if validation_errors:
            for err in validation_errors:
                if err.path:
                    field = '-'.join([str(e) for e in err.path])
                elif err.schema_path:
                    field = '-'.join([str(e) for e in err.schema_path])
                else:
                    field = 'error'

                if field in exception_msgs:
                    exception_msgs[field].append(err.message)
                else:
                    exception_msgs[field] = [err.message]

        if not is_valid and raise_error:
            raise OdsException("\nCombine config validation error: {}".format(
                json.dumps(exception_msgs, indent=4)
            ))

        return is_valid, exception_msgs


def read_config(config_path):
    """
    Read config and add defaults where necessary. Performs validation against
    config schema.
    """
    with open(config_path, 'r') as f:
        config = json.load(f)
    return prepare_config(config)


def prepare_config(config, raise_error=True):
    config = DEFAULT_CONFIG | config
    CombineSettingsSchema().validate(config, raise_error=raise_error)
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
            output_type='csv',
            **kwargs
            ):
    # prepare output dir
    if output_dir is None:
        output_dir = get_default_output_dir()

    output_dir = Path(output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)
    logger.info(f'Output directory: {output_dir}')
    logger.debug(f'Config: num_periods={group_number_of_periods}, mean={group_mean}, '
                 f'secondary_uncertainty={group_secondary_uncertainty}, '
                 f'format_priority={group_format_priority}, plt={group_plt}, '
                 f'alt={group_alt}, ept={group_ept}')

    # Group meta data
    logger.info("Stage 1/5: Loading analysis directories")
    logger.debug(f'Analysis dirs ({len(analysis_dirs)}): {analysis_dirs}')
    analyses = load_analysis_dirs(analysis_dirs)
    logger.debug(f'Stage 1a done: loaded {len(analyses)} analyses')

    logger.info("Stage 1/5: Creating combine group")
    group, groupset_summaryinfo = create_combine_group(analyses,
                                                       groupeventset_fields=group_event_set_fields)
    logger.debug(f'Stage 1b done: groupset has {len(group.groupset)} entries')

    logger.info("Stage 1/5: Saving summary info")
    save_summary_info(groupset_summaryinfo, group.groupset, output_dir)
    logger.debug('Stage 1c done: summary info saved')

    # Period sampling
    logger.info("Stage 2/5: Period Sampling")
    logger.debug(f'max_group_periods={group_number_of_periods}, occ_dtype={occ_dtype}')
    group_period = generate_group_periods(group,
                                          max_group_periods=group_number_of_periods,
                                          occ_dtype=occ_dtype
                                          )
    logger.debug('Stage 2 done: group periods generated')

    # Loss sampling
    no_quantile_sampling = group_mean and not group_secondary_uncertainty
    logger.info("Stage 3/5: Quantile Sampling")
    logger.debug(f'no_quantile_sampling={no_quantile_sampling}, correlation={group_correlation}')
    gpqt = generate_gpqt(group_period, group,
                         no_quantile_sampling=no_quantile_sampling,
                         correlation=group_correlation
                         )
    logger.debug('Stage 3 done: quantile periods generated')

    logger.info("Stage 4/5: Loss Sampling")
    logger.debug(f'mean_only={group_mean}, secondary_uncertainty={group_secondary_uncertainty}, '
                 f'parametric_distribution={group_parametric_distribution}')
    gplt = do_loss_sampling(gpqt, group,
                            mean_only=group_mean,
                            secondary_uncertainty=group_secondary_uncertainty,
                            parametric_distribution=group_parametric_distribution,
                            format_priority=group_format_priority
                            )
    logger.debug('Stage 4 done: loss sampling complete')

    # Output generation
    logger.info("Stage 5/5: Output Generation")

    outputs = []

    if group_plt:
        outputs.append(('gplt', gplt[GPLT_headers], GPLT_schema))

    if group_alt:
        logger.debug('Generating ALT')
        outputs.append(('galt', generate_alt(gplt, group_number_of_periods), GALT_schema))
        logger.debug('ALT generated')

    if group_ept:
        logger.debug(f'Generating EPT (oep={group_ept_oep}, aep={group_ept_aep})')
        outputs.append(('gept',
                        generate_ept(gplt, group_number_of_periods,
                                     oep=group_ept_oep,
                                     aep=group_ept_aep), GEPT_schema))
        logger.debug('EPT generated')

    for output_name, output_df, output_schema in outputs:
        logger.debug(f'Saving {output_name}.{output_type}')
        save_output(output_df, output_dir, output_name,
                    output_type=output_type, schema=output_schema)
        logger.debug(f'Saved {output_name}.{output_type}')
    logger.info("Stage 5/5: Output Generation complete")


if __name__ == "__main__":
    config_path = Path(Path(__file__).parent / 'config.json')
    config = read_config(config_path)
    output = combine(**config)
