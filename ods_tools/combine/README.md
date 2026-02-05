# ODS Tools Combine: Combining ORD Results

This document describes how to use the `ods_tools combine` functionality to aggregate Open Results Data (ORD) from multiple catastrophe loss model analyses into consolidated risk metrics.

## Overview

The combine module implements a methodology for "rolling up" results from multiple analyses to support:

- **Portfolio aggregation** across models and geographies
- **Regulatory submission** of consolidated EP curves
- **Enterprise risk management** including roll-up across subsidiaries and lines of business

The methodology combines results at a detailed level (event loss tables and period loss tables) using a random, repeatable sampling method that preserves correlation structures and can accommodate models from different suppliers with varying structures and outputs.

## Key Concepts


| Term | Description |
|------|-------------|
| **Results roll-up** | Combining multiple sets of results into one consolidated result |
| **GroupEventSet** | A subset of analyses that share common event/occurrence definitions |
| **GroupSet** | Output summary levels available for each group (perspective + summary level) |
| **GroupPeriod** | The common period timeline used for combining results |
| **GPLT** | Group Period Loss Table - detailed output with one loss per period per event |
| **GALT** | Group Average Loss Table - AAL with mean and standard deviation |
| **GEPT** | Group Exceedance Probability Table - OEP and AEP curves |


## Input Requirements

### Analysis Directory Structure

Each analysis directory must contain:

```
analysis_dir/
├── analysis_settings.json    # Analysis configuration with model settings
├── input/
│   └── occurrence.bin        # Binary occurrence file (event-period mapping)
└── output/
    ├── gul_S1_summary-info.csv    # Summary info per summary level
    ├── gul_S1_melt.csv            # Moment ELT (required for mean-only)
    ├── gul_S1_qelt.csv            # Quantile ELT (optional)
    ├── gul_S1_selt.csv            # Sample ELT (optional)
    └── ...
```

### Required Files

| File | Purpose |
|------|---------|
| `analysis_settings.json` | Contains model settings, perspectives, and summary level definitions |
| `occurrence.bin` | Binary file mapping events to periods |
| `*_melt.csv` | Required for mean-only loss sampling |
| `*_summary-info.csv` | Summary level metadata (TIV, OED fields) |

### Optional ELT Files for Secondary Uncertainty

For secondary uncertainty sampling, the module can use (in priority order):

1. **MELT** (Moment ELT): Uses parametric sampling (beta distribution by default)
2. **QELT** (Quantile ELT): Uses linear interpolation between quantiles
3. **SELT** (Sample ELT): Uses rank-based sample selection (note `number_of_samples` is a required field in `analysis_settings.json`)

## Configuration

Create a JSON configuration file with the following options:

### Required Parameters

```json
{
    "group_number_of_periods": 100000
}
```

### Full Configuration Example

```json
{
    "group_number_of_periods": 100000,
    "group_period_seed": 2479,
    "group_mean": true,
    "group_secondary_uncertainty": true,
    "group_parametric_distribution": "beta",
    "group_format_priority": ["M", "Q", "S"],
    "group_correlation": null,
    "group_fill_perspectives": false,
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
    "group_plt": true,
    "group_alt": true,
    "group_ept": true,
    "group_ept_oep": true,
    "group_ept_aep": true
}
```

### Configuration Parameters

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `group_number_of_periods` | int | **required** | Number of periods for the grouped result (typically >= max periods in any analysis) |
| `group_period_seed` | int | 2479 | Random seed for period sampling (enables repeatability) |
| `group_mean` | bool | false | Output grouped mean results (LossType 1 or 3) |
| `group_secondary_uncertainty` | bool | false | Output grouped results with secondary uncertainty (LossType 2) |
| `group_parametric_distribution` | str | "beta" | Distribution for MELT sampling (`"beta"`) |
| `group_format_priority` | list | ["M","Q","S"] | Priority order for ELT formats: M=Moment, Q=Quantile, S=Sample |
| `group_correlation` | float | null | Correlation factor (0.0=uncorrelated, 1.0=fully correlated) |
| `group_fill_perspectives` | bool | false | Fill missing perspectives (e.g., use gross for missing net) |
| `group_event_set_fields` | list | see default | Fields used to identify common event sets across analyses |
| `group_plt` | bool | false | Output Group Period Loss Table |
| `group_alt` | bool | false | Output Group Average Loss Table (AAL) |
| `group_ept` | bool | false | Output Group Exceedance Probability Table |
| `group_ept_oep` | bool | true | Include OEP in EPT output (if group_ept=true) |
| `group_ept_aep` | bool | true | Include AEP in EPT output (if group_ept=true) |

## Usage

### Command Line Interface

```bash
ods_tools combine \
    --analysis-dirs /path/to/analysis1 /path/to/analysis2 \
    --config-file /path/to/combine_config.json \
    --output-dir /path/to/output \
    -v 20  # logging level (10=debug, 20=info, 30=warning)
```

### Python API

```python
from ods_tools.combine.combine import combine, read_config

# Load configuration
config = read_config('/path/to/combine_config.json')

# Run combine
combine(
    analysis_dirs=['/path/to/analysis1', '/path/to/analysis2'],
    output_dir='/path/to/output',
    **config
)
```

### Programmatic Configuration

```python
from ods_tools.combine.combine import combine

combine(
    analysis_dirs=['/path/to/analysis1', '/path/to/analysis2'],
    group_number_of_periods=100000,
    group_mean=True,
    group_secondary_uncertainty=True,
    group_alt=True,
    group_ept=True,
    group_plt=False,  # PLT can be very large
    output_dir='/path/to/output'
)
```

## Output Files

The combine module generates outputs per GroupSet (unique combination of perspective + summary level):

```
/path/to/output/
├── gul_GS0_summary-info.csv   # Grouped summary info
├── gul_GS1_summary-info.csv
├── 0_plt.csv                   # Group Period Loss Table (if group_plt=true)
├── 0_alt.csv                   # Group Average Loss Table (if group_alt=true)
├── 0_ept.csv                   # Group EP Table (if group_ept=true)
├── 1_plt.csv
├── 1_alt.csv
└── 1_ept.csv
```

### Output Schemas

#### Group Period Loss Table (GPLT)

| Column | Type | Description |
|--------|------|-------------|
| groupset_id | int | GroupSet identifier |
| outputset_id | int | Source OutputSet identifier |
| SummaryId | int | Aligned summary identifier |
| GroupPeriod | int | Group period number (1 to group_number_of_periods) |
| Period | int | Original analysis period |
| groupeventset_id | int | GroupEventSet identifier |
| EventId | int | Event identifier |
| Loss | float | Sampled loss value |
| LossType | int | Loss type (1, 2, or 3) |

#### Group Average Loss Table (GALT)

| Column | Type | Description |
|--------|------|-------------|
| groupset_id | int | GroupSet identifier |
| SummaryId | int | Summary identifier |
| LossType | int | Loss type |
| Mean | float | Average Annual Loss |
| Std | float | Standard deviation |

#### Group EP Table (GEPT)

| Column | Type | Description |
|--------|------|-------------|
| groupset_id | int | GroupSet identifier |
| SummaryId | int | Summary identifier |
| EPCalc | int | EP calculation type (1=OEP, 3=AEP) |
| EPType | int | EP type (same as LossType) |
| RP | float | Return Period |
| Loss | float | Loss at return period |

## Methodology

### Step 1: Grouping

The module identifies compatible analyses by:

1. **OutputSet grouping**: Analyses with matching `perspective_code` and `exposure_summary_level_fields` are grouped into a **GroupSet**
2. **EventSet grouping**: Analyses sharing common event definitions (based on `group_event_set_fields`) form a **GroupEventSet**
3. **SummaryId alignment**: SummaryIds are remapped to ensure consistency across analyses

### Step 2: Period Sampling

Period sampling assigns events from multiple analyses to a common group period timeline:

- **Random**: Uses random numbers to select periods, removing model-driven patterns
- **Repeatable**: Same seed produces identical results
- **Without replacement**: Each period is sampled once per cycle before repeating

### Step 3: Loss Sampling

Two sampling modes are available:

#### Mean Only (`group_mean=true`)

Uses MeanLoss values from MELT files directly. No random sampling involved.

#### Secondary Uncertainty (`group_secondary_uncertainty=true`)

Generates random quantiles and samples from the loss distribution:

1. **From MELT**: Fits a beta distribution using Mean, SD, and MaxLoss, then samples
2. **From QELT**: Interpolates between quantile points
3. **From SELT**: Selects a sample based on quantile rank

### Step 4: Output Generation

#### AAL Calculation

```
Mean = Sum(Loss) / group_number_of_periods
Std = sqrt(Sum((Mean - Loss)^2) / (group_number_of_periods - 1))
```

#### EP Curve Calculation

**OEP** (Occurrence):
1. Group by GroupPeriod, SummaryId, LossType
2. Take maximum loss per GroupPeriod
3. Rank losses and compute Return Period = N / Rank

**AEP** (Aggregate):
1. Group by GroupPeriod, SummaryId, LossType
2. Sum losses per GroupPeriod
3. Rank and compute Return Period

## Correlation

The `group_correlation` parameter controls how random quantiles are generated:

| Value | Behavior |
|-------|----------|
| `null` or `0.0` | Independent random numbers for each event occurrence |
| `1.0` | Fully correlated - same quantile for all losses from same event |
| `0.0 < x < 1.0` | Partial correlation using Gaussian copula |

## Examples

### Basic Mean-Only Combining

```json
{
    "group_number_of_periods": 50000,
    "group_mean": true,
    "group_alt": true,
    "group_ept": true
}
```

```bash
ods_tools combine \
    -a ./run1 ./run2 ./run3 \
    --config-file mean_config.json \
    --output-dir ./combined
```

### Full Uncertainty with Beta Sampling

```json
{
    "group_number_of_periods": 100000,
    "group_secondary_uncertainty": true,
    "group_parametric_distribution": "beta",
    "group_format_priority": ["M"],
    "group_alt": true,
    "group_ept": true,
    "group_plt": false
}
```

### Combining with Correlation

```json
{
    "group_number_of_periods": 100000,
    "group_mean": true,
    "group_secondary_uncertainty": true,
    "group_correlation": 0.5,
    "group_event_set_fields": [
        "model_supplier_id",
        "model_name_id",
        "event_set_id",
        "event_occurrence_id"
    ],
    "group_alt": true,
    "group_ept": true
}
```

## Error Handling

Common errors and solutions:

| Error | Cause | Solution |
|-------|-------|----------|
| `No loss sampling specified` | Neither `group_mean` nor `group_secondary_uncertainty` is true | Set at least one to true |
| `Mean only can only be performed if melt files present` | Missing MELT files | Generate MELT files in source analyses |
| `Currently does not support different max_periods in a group` | Analyses have different period counts | Ensure analyses in same GroupEventSet have same max_periods |
| `Number of samples not provided` | SELT sampling without `number_of_samples` in analysis settings | Add `number_of_samples` to analysis_settings.json |

## Best Practices

1. **Period Count**: Set `group_number_of_periods` >= largest `max_periods` in any analysis
2. **Reproducibility**: Document the `group_period_seed` used for auditing
3. **Memory**: Avoid `group_plt=true` for large runs - PLT files can be very large
4. **Validation**: Compare grouped AAL against weighted sum of individual AALs
5. **Event Sets**: Use consistent event set definitions across analyses you want to combine

## References

- [Combining Results in ORD v1.1](https://github.com/OasisLMF/ODS_OpenExposureData) - Full methodology specification
- [ORD Requirements for Results Processing](https://github.com/OasisLMF/ODS_OpenExposureData) - Use cases and requirements
