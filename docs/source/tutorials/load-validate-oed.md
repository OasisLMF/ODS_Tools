---
file_format: mystnb
kernelspec:
  name: python3
  display_name: Python 3
---

# Load and validate an OED exposure

`ods_tools` reads OED (Open Exposure Data) files into typed pandas DataFrames and
**validates** them against the OED standard. This notebook loads a location file,
runs validation, catches an issue, fixes it, and re-validates.

```{note}
Executable notebook — the cells below run the `ods_tools` **library** at docs-build
time (fast, no model run), so the outputs always reflect the current code and OED
schema.
```

```{code-cell} python
import warnings; warnings.filterwarnings("ignore")
from pathlib import Path
import ods_tools.oed as oed

_c = [Path("data/oed"), Path("tutorials/data/oed"), Path("docs/source/tutorials/data/oed")]
DATA = next((c for c in _c if c.exists()), None)
assert DATA is not None, "OED example data not found"
LOCATION = DATA / "SourceLocOEDPiWind10Currency.csv"
```

## Load an OED location file

`OedExposure` loads each OED source (location, account, reinsurance) into a typed
DataFrame — column data types follow the OED specification.

```{code-cell} python
exposure = oed.OedExposure(location=str(LOCATION))
loc = exposure.location.dataframe
print(f"{loc.shape[0]} locations, {loc.shape[1]} columns")
loc[["PortNumber", "AccNumber", "LocNumber", "CountryCode",
     "OccupancyCode", "ConstructionCode", "BuildingTIV"]].head()
```

## Validate against the OED standard

`ods_tools` ships the OED validation rules (required/conditional fields, valid code
lists, peril codes, …). We run them in **return** mode so the findings come back as
data instead of raising:

```{code-cell} python
from ods_tools.oed.common import DEFAULT_VALIDATION_CONFIG

return_config = [{**check, "on_error": "return"} for check in DEFAULT_VALIDATION_CONFIG]
findings = exposure.check(return_config)
print(f"{len(findings)} validation finding(s)")
for f in findings:
    print(f"- [{f['name']}] {f['msg'].splitlines()[0]}")
```

This example file is missing a **conditionally required** column: OED requires a
peril to be specified (`LocPeril`) when perils-related terms are present.

## Fix and re-validate

Add the missing peril (PiWind is a windstorm model, peril `WW1`) and re-run
validation:

```{code-cell} python
exposure.location.dataframe["LocPeril"] = "WW1"
findings = exposure.check(return_config)
print(f"{len(findings)} validation finding(s) after fix")
```

## Enforcing validation

Passing `check_oed=True` (or `on_error='raise'` in the config) makes `ods_tools`
**raise** on the first failing check instead of returning — this is what the CLI does:

```bash
ods_tools check --location SourceLocOEDPiWind10Currency.csv
```

## Where next

- **ODTF** — transform other exposure formats (e.g. AIR CEDE) into OED.
- **Currency conversion** — convert a multi-currency exposure to a reporting currency.
- The OED field definitions and code lists (the *standard*) are single-sourced in the
  `ODS_OpenExposureData` repository.
