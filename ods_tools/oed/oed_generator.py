"""
OED Test Data Generator
=======================

Generates synthetic OED (Open Exposure Data) file combinations (Loc, Acc, ReinsInfo, ReinsScope)
for testing purposes. Driven by a JSON configuration file that specifies file sizes, fields,
financial terms, OED version, and data generation strategies.

Designed for eventual incorporation into ODS Tools (https://github.com/OasisLMF/ODS_Tools).

Usage:
    python oed_generator.py --config config.json --output-dir ./output

Author: Oasis LMF
License: BSD-2-Clause
"""

import argparse
import json
import logging
import os
import random
from datetime import datetime, timedelta
from typing import Any, Optional

import pandas as pd

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# OED Schema Access
# ---------------------------------------------------------------------------


def load_oed_schema(oed_version: str) -> dict:
    """Load the OED schema for a given version using the OedSchema class."""
    from ods_tools.oed.oed_schema import OedSchema

    try:
        oed_schema = OedSchema.from_oed_schema_info(oed_version)
    except (FileNotFoundError, Exception) as e:
        available = get_available_oed_versions()
        raise FileNotFoundError(
            f"OED version '{oed_version}' not found. Available: {available}"
        ) from e

    return oed_schema.schema


def get_available_oed_versions() -> list[str]:
    """Return a sorted list of OED versions available in the ods_tools package."""
    from glob import glob as _glob

    from ods_tools.oed.oed_schema import OedSchema

    import re
    data_pattern = OedSchema.DEFAULT_ODS_SCHEMA_PATH.replace('{}', '*')
    spec_files = _glob(data_pattern)
    version_pattern = re.compile(r"OpenExposureData_(\d+\.\d+\.\d+)Spec\.json")
    return sorted(
        m.group(1) for f in spec_files
        if (m := version_pattern.search(f))
    )


# ---------------------------------------------------------------------------
# Constants & Reference Data
# ---------------------------------------------------------------------------

# OED file type keys (matching the spec's input_fields keys)
FILE_TYPE_MAP = {
    "loc": "Loc",
    "location": "Loc",
    "acc": "Acc",
    "account": "Acc",
    "ri_info": "ReinsInfo",
    "reinsinfo": "ReinsInfo",
    "ri_scope": "ReinsScope",
    "reinsscope": "ReinsScope",
}

# Peril codes commonly used in OED
PERIL_CODES = {
    "WTC": "Tropical Cyclone",
    "WEC": "Extra-Tropical Cyclone",
    "WSS": "Storm Surge",
    "WW1": "Winterstorm Wind",
    "WW2": "Winterstorm Surge",
    "QEQ": "Earthquake",
    "QFF": "Fire Following",
    "QLF": "Liquefaction",
    "QSL": "Landslide (EQ-induced)",
    "QTS": "Tsunami",
    "ORF": "River/Fluvial Flood",
    "OSF": "Surface/Pluvial Flood",
    "OO1": "Flood",
    "ZZ1": "Wildfire",
    "BBF": "Bushfire",
    "AA1": "All Perils",
}

# Reinsurance type codes
REINS_TYPES = {
    "SS": "Surplus Share",
    "QS": "Quota Share",
    "FAC": "Facultative",
    "PR": "Per Risk XL",
    "CXL": "Catastrophe XL",
    "AXL": "Aggregate XL",
}

# Risk level codes
RISK_LEVELS = {
    "LOC": "Location",
    "POL": "Policy",
    "ACC": "Account",
    "LGR": "Location Group",
}

# Common currencies
CURRENCIES = ["USD", "GBP", "EUR", "JPY", "AUD", "CAD", "CHF", "HKD", "SGD"]

# Ded/Limit type codes
DED_LIMIT_TYPES = {
    0: "None",
    1: "Monetary amount",
    2: "Percentage of TIV",
}


# ---------------------------------------------------------------------------
# Data Generators
# ---------------------------------------------------------------------------

class DataGenerator:
    """Generates random or fixed data values for OED fields."""

    def __init__(self, schema: dict, config: dict, seed: Optional[int] = None):
        self.schema = schema
        self.config = config
        self.rng = random.Random(seed)

        # Pre-extract useful reference data from schema
        self.occupancy_codes = list(schema.get("occupancy", {}).keys())
        self.construction_codes = list(schema.get("construction", {}).keys())
        self.country_codes = list(schema.get("country", {}).keys())

        # Filter to common ISO country codes (2-letter, no offshore codes)
        self.iso_country_codes = [
            c for c in self.country_codes
            if len(c) == 2 and c.isalpha() and c.isupper()
        ]

        # Global overrides from config
        self.global_overrides = config.get("global_defaults", {})

    def _get_override(self, field_name: str, file_type: str) -> Any:
        """Check if a fixed value override exists for this field."""
        # Check file-specific overrides first
        file_cfg = self.config.get("files", {}).get(file_type, {})
        fixed_values = file_cfg.get("fixed_values", {})
        if field_name in fixed_values:
            return fixed_values[field_name]

        # Then global overrides
        if field_name in self.global_overrides:
            return self.global_overrides[field_name]

        return None

    def generate_value(self, field_name: str, field_spec: dict, file_type: str,
                       row_idx: int, context: dict) -> Any:
        """Generate a value for a given OED field.

        Args:
            field_name: The OED field name (e.g. 'PortNumber')
            field_spec: The field specification from the OED schema
            file_type: The file type ('Loc', 'Acc', 'ReinsInfo', 'ReinsScope')
            row_idx: The current row index (0-based)
            context: Dict of context data (e.g. generated PortNumbers, AccNumbers etc.)
        """
        # Check for explicit override
        override = self._get_override(field_name, file_type)
        if override is not None:
            if isinstance(override, list):
                return self.rng.choice(override)
            return override

        # Dispatch to specific generators based on field name
        gen_method = getattr(self, f"_gen_{field_name.lower()}", None)
        if gen_method:
            return gen_method(field_spec, file_type, row_idx, context)

        # Fall back to type-based generation
        return self._gen_by_dtype(field_name, field_spec, row_idx)

    # --- Key field generators ---

    def _gen_portnumber(self, spec, file_type, row_idx, ctx):
        if file_type in ("Loc", "Acc"):
            return ctx.get("port_numbers", ["P1"])[row_idx % len(ctx.get("port_numbers", ["P1"]))]
        if file_type in ("ReinsScope",):
            ports = ctx.get("port_numbers", ["P1"])
            return self.rng.choice(ports)
        return ctx.get("port_numbers", ["P1"])[0]

    def _gen_accnumber(self, spec, file_type, row_idx, ctx):
        if file_type == "Loc":
            return ctx.get("acc_numbers", [f"A{row_idx + 1}"])[row_idx % len(ctx.get("acc_numbers", [f"A{row_idx + 1}"]))]
        if file_type == "Acc":
            return ctx.get("acc_numbers", [f"A{row_idx + 1}"])[row_idx % len(ctx.get("acc_numbers", [f"A{row_idx + 1}"]))]
        if file_type == "ReinsScope":
            accs = ctx.get("acc_numbers", ["A1"])
            return self.rng.choice(accs)
        return f"A{row_idx + 1}"

    def _gen_locnumber(self, spec, file_type, row_idx, ctx):
        if file_type == "ReinsScope":
            locs = ctx.get("loc_numbers", [])
            return self.rng.choice(locs) if locs else ""
        return f"L{row_idx + 1:05d}"

    def _gen_polnumber(self, spec, file_type, row_idx, ctx):
        if file_type == "Acc":
            return ctx.get("pol_numbers", [f"POL{row_idx + 1}"])[row_idx % len(ctx.get("pol_numbers", [f"POL{row_idx + 1}"]))]
        if file_type == "ReinsScope":
            pols = ctx.get("pol_numbers", [])
            return self.rng.choice(pols) if pols else ""
        return f"POL{row_idx + 1}"

    def _gen_countrycode(self, spec, file_type, row_idx, ctx):
        country_pool = ctx.get("country_pool")
        if country_pool:
            return self.rng.choice(country_pool)
        if self.iso_country_codes:
            return self.rng.choice(self.iso_country_codes[:50])  # Top 50
        return "US"

    def _gen_locperilscovered(self, spec, file_type, row_idx, ctx):
        return ctx.get("peril_code", "WTC")

    def _gen_locperil(self, spec, file_type, row_idx, ctx):
        return ctx.get("peril_code", "WTC")

    def _gen_polperilscovered(self, spec, file_type, row_idx, ctx):
        return ctx.get("peril_code", "WTC")

    def _gen_polperil(self, spec, file_type, row_idx, ctx):
        return ctx.get("peril_code", "WTC")

    def _gen_accperil(self, spec, file_type, row_idx, ctx):
        return ctx.get("peril_code", "WTC")

    def _gen_condperil(self, spec, file_type, row_idx, ctx):
        return ctx.get("peril_code", "WTC")

    def _gen_reinsperil(self, spec, file_type, row_idx, ctx):
        return ctx.get("peril_code", "AA1")

    def _gen_loccurrency(self, spec, file_type, row_idx, ctx):
        return ctx.get("currency", "USD")

    def _gen_acccurrency(self, spec, file_type, row_idx, ctx):
        return ctx.get("currency", "USD")

    def _gen_reinscurrency(self, spec, file_type, row_idx, ctx):
        return ctx.get("currency", "USD")

    # --- TIV generators ---

    def _gen_buildingtiv(self, spec, file_type, row_idx, ctx):
        tiv_range = ctx.get("tiv_range", [100000, 10000000])
        return round(self.rng.uniform(tiv_range[0], tiv_range[1]), 2)

    def _gen_othertiv(self, spec, file_type, row_idx, ctx):
        tiv_range = ctx.get("tiv_range", [100000, 10000000])
        return round(self.rng.uniform(0, tiv_range[1] * 0.2), 2)

    def _gen_contentstiv(self, spec, file_type, row_idx, ctx):
        tiv_range = ctx.get("tiv_range", [100000, 10000000])
        return round(self.rng.uniform(0, tiv_range[1] * 0.5), 2)

    def _gen_bitiv(self, spec, file_type, row_idx, ctx):
        tiv_range = ctx.get("tiv_range", [100000, 10000000])
        return round(self.rng.uniform(0, tiv_range[1] * 0.3), 2)

    # --- Location characteristic generators ---

    def _gen_occupancycode(self, spec, file_type, row_idx, ctx):
        if self.occupancy_codes:
            return int(self.rng.choice(self.occupancy_codes[:30]))  # Common codes
        return 1000

    def _gen_constructioncode(self, spec, file_type, row_idx, ctx):
        if self.construction_codes:
            return int(self.rng.choice(self.construction_codes[:20]))
        return 5000

    def _gen_numberofstoreys(self, spec, file_type, row_idx, ctx):
        return self.rng.randint(1, 50)

    def _gen_yearbuilt(self, spec, file_type, row_idx, ctx):
        return self.rng.randint(1950, 2025)

    def _gen_latitude(self, spec, file_type, row_idx, ctx):
        lat_range = ctx.get("latitude_range", [-90, 90])
        return round(self.rng.uniform(lat_range[0], lat_range[1]), 6)

    def _gen_longitude(self, spec, file_type, row_idx, ctx):
        lon_range = ctx.get("longitude_range", [-180, 180])
        return round(self.rng.uniform(lon_range[0], lon_range[1]), 6)

    def _gen_postalcode(self, spec, file_type, row_idx, ctx):
        return f"{self.rng.randint(10000, 99999)}"

    def _gen_city(self, spec, file_type, row_idx, ctx):
        cities = ["London", "New York", "Tokyo", "Paris", "Munich", "Zurich",
                  "Sydney", "Toronto", "Hamilton", "Singapore", "Miami", "Houston"]
        return self.rng.choice(cities)

    def _gen_streetaddress(self, spec, file_type, row_idx, ctx):
        return f"{self.rng.randint(1, 9999)} {self.rng.choice(['High', 'Main', 'Oak', 'Elm', 'Park', 'Lake'])} Street"

    # --- Financial term generators (deductibles, limits) ---

    def _gen_financial_ded(self, spec, file_type, row_idx, ctx):
        """Generate a deductible value based on configured financial terms."""
        fin_config = ctx.get("financial_terms", {})
        ded_range = fin_config.get("deductible_range", [1000, 100000])
        return round(self.rng.uniform(ded_range[0], ded_range[1]), 2)

    def _gen_financial_limit(self, spec, file_type, row_idx, ctx):
        """Generate a limit value."""
        fin_config = ctx.get("financial_terms", {})
        limit_range = fin_config.get("limit_range", [500000, 50000000])
        return round(self.rng.uniform(limit_range[0], limit_range[1]), 2)

    def _gen_financial_ded_type(self, spec, file_type, row_idx, ctx):
        """Generate a deductible type code."""
        return self.rng.choice([0, 1, 2])

    def _gen_financial_ded_code(self, spec, file_type, row_idx, ctx):
        return 0  # Standard deductible

    # Catch-all for Loc/Acc deductible and limit fields
    def _gen_financial_field(self, field_name, spec, file_type, row_idx, ctx):
        fn_lower = field_name.lower()
        if "dedtype" in fn_lower or "limittype" in fn_lower:
            return self._gen_financial_ded_type(spec, file_type, row_idx, ctx)
        if "dedcode" in fn_lower or "limitcode" in fn_lower:
            return self._gen_financial_ded_code(spec, file_type, row_idx, ctx)
        if "mindex" in fn_lower or "maxded" in fn_lower:
            return 0
        if "ded" in fn_lower:
            return self._gen_financial_ded(spec, file_type, row_idx, ctx)
        if "limit" in fn_lower:
            return self._gen_financial_limit(spec, file_type, row_idx, ctx)
        return 0

    # --- Reinsurance field generators ---

    def _gen_reinsnumber(self, spec, file_type, row_idx, ctx):
        if file_type == "ReinsScope":
            reins_numbers = ctx.get("reins_numbers", [1])
            return self.rng.choice(reins_numbers)
        return row_idx + 1

    def _gen_reinslayernumber(self, spec, file_type, row_idx, ctx):
        return 1

    def _gen_reinstype(self, spec, file_type, row_idx, ctx):
        reins_types = ctx.get("reins_types", list(REINS_TYPES.keys()))
        return self.rng.choice(reins_types)

    def _gen_risklevel(self, spec, file_type, row_idx, ctx):
        return self.rng.choice(list(RISK_LEVELS.keys()))

    def _gen_placedpercent(self, spec, file_type, row_idx, ctx):
        return round(self.rng.uniform(0.5, 1.0), 4)

    def _gen_cededpercent(self, spec, file_type, row_idx, ctx):
        return round(self.rng.uniform(0.1, 1.0), 4)

    def _gen_inuringpriority(self, spec, file_type, row_idx, ctx):
        return self.rng.randint(1, 3)

    def _gen_risklimit(self, spec, file_type, row_idx, ctx):
        return round(self.rng.uniform(0, 10000000), 2)

    def _gen_riskattachment(self, spec, file_type, row_idx, ctx):
        return round(self.rng.uniform(0, 5000000), 2)

    def _gen_occlimit(self, spec, file_type, row_idx, ctx):
        return round(self.rng.uniform(0, 100000000), 2)

    def _gen_occattachment(self, spec, file_type, row_idx, ctx):
        return round(self.rng.uniform(0, 50000000), 2)

    def _gen_treatyshare(self, spec, file_type, row_idx, ctx):
        return round(self.rng.uniform(0.1, 1.0), 4)

    def _gen_reinsname(self, spec, file_type, row_idx, ctx):
        return f"Treaty_{row_idx + 1}"

    def _gen_reinsinceptiondate(self, spec, file_type, row_idx, ctx):
        base = datetime(2025, 1, 1) + timedelta(days=self.rng.randint(0, 365))
        return base.strftime("%Y-%m-%d")

    def _gen_reinsexpirydate(self, spec, file_type, row_idx, ctx):
        base = datetime(2026, 1, 1) + timedelta(days=self.rng.randint(0, 365))
        return base.strftime("%Y-%m-%d")

    # --- Date generators ---

    def _gen_locexpirydate(self, spec, file_type, row_idx, ctx):
        return (datetime(2026, 1, 1) + timedelta(days=self.rng.randint(0, 365))).strftime("%Y-%m-%d")

    def _gen_locinceptiondate(self, spec, file_type, row_idx, ctx):
        return (datetime(2025, 1, 1) + timedelta(days=self.rng.randint(0, 365))).strftime("%Y-%m-%d")

    # --- OED Version ---

    def _gen_oedversion(self, spec, file_type, row_idx, ctx):
        return ctx.get("oed_version", "5.0.0")

    # --- Generic dtype-based generator ---

    def _gen_by_dtype(self, field_name: str, field_spec: dict, row_idx: int) -> Any:
        """Generate a value based on the field's data type."""
        dtype = field_spec.get("Data Type", "").lower()
        default = field_spec.get("Default", "")

        # Check if it's a financial field by name pattern
        fn_lower = field_name.lower()
        if any(p in fn_lower for p in ["ded", "limit", "attach", "share"]):
            if "type" in fn_lower or "code" in fn_lower:
                return 0
            return 0.0

        if "float" in dtype:
            if default and default not in ("n/a", ""):
                try:
                    return float(default)
                except (ValueError, TypeError):
                    pass
            return 0.0

        if "int" in dtype or "tinyint" in dtype:
            if default and default not in ("n/a", ""):
                try:
                    return int(default)
                except (ValueError, TypeError):
                    pass
            return 0

        if "char" in dtype or "varchar" in dtype:
            if default and default not in ("n/a", ""):
                return str(default).strip("()'\"")
            return ""

        return ""


# ---------------------------------------------------------------------------
# File Generator
# ---------------------------------------------------------------------------

class OEDFileGenerator:
    """Generates OED test data files from a configuration."""

    def __init__(self, config: dict):
        """
        Args:
            config: Parsed JSON configuration dict.
        """
        self.config = config
        self.oed_version = config.get("oed_version", "5.0.0")
        self.schema = load_oed_schema(self.oed_version)
        self.seed = config.get("seed", None)
        self.generator = DataGenerator(self.schema, config, seed=self.seed)

        # Build the generation context
        self._build_context()

    def _build_context(self):
        """Build shared context for cross-file referential integrity."""
        global_cfg = self.config.get("global_defaults", {})
        files_cfg = self.config.get("files", {})

        # Determine portfolio structure
        num_ports = global_cfg.get("num_portfolios", 1)
        num_accounts_per_port = global_cfg.get("num_accounts_per_portfolio", 5)
        num_pols_per_account = global_cfg.get("num_policies_per_account", 1)

        self.port_numbers = [f"P{i + 1}" for i in range(num_ports)]

        # Generate account numbers (flat list)
        self.acc_numbers = []
        self.acc_to_port = {}
        for p_idx, port in enumerate(self.port_numbers):
            for a_idx in range(num_accounts_per_port):
                acc = f"A{p_idx * num_accounts_per_port + a_idx + 1}"
                self.acc_numbers.append(acc)
                self.acc_to_port[acc] = port

        # Generate policy numbers
        self.pol_numbers = []
        self.pol_to_acc = {}
        for acc in self.acc_numbers:
            for pol_idx in range(num_pols_per_account):
                pol = f"POL{acc[1:]}_{pol_idx + 1}" if num_pols_per_account > 1 else f"POL{acc[1:]}"
                self.pol_numbers.append(pol)
                self.pol_to_acc[pol] = acc

        # Build shared context dict
        self.context = {
            "port_numbers": self.port_numbers,
            "acc_numbers": self.acc_numbers,
            "pol_numbers": self.pol_numbers,
            "acc_to_port": self.acc_to_port,
            "pol_to_acc": self.pol_to_acc,
            "loc_numbers": [],  # Will be populated during Loc generation
            "reins_numbers": [],  # Will be populated during RI generation
            "peril_code": global_cfg.get("peril_code", "WTC"),
            "currency": global_cfg.get("currency", "USD"),
            "oed_version": self.oed_version,
            "tiv_range": global_cfg.get("tiv_range", [100000, 10000000]),
            "latitude_range": global_cfg.get("latitude_range", [25.0, 55.0]),
            "longitude_range": global_cfg.get("longitude_range", [-125.0, -65.0]),
            "country_pool": global_cfg.get("country_codes", None),
            "financial_terms": global_cfg.get("financial_terms", {}),
        }

    def _resolve_fields(self, file_type: str) -> list[str]:
        """Determine which fields to include for a given file type.

        Respects the config's field lists, financial term inclusion, and
        the 'include_required' flag.
        """
        spec_key = FILE_TYPE_MAP.get(file_type.lower(), file_type)
        all_fields = self.schema["input_fields"].get(spec_key, {})

        file_cfg = self.config.get("files", {}).get(file_type, {})

        # Start with explicitly listed fields
        explicit_fields = file_cfg.get("fields", [])

        # Should we include all required fields?
        include_required = file_cfg.get("include_required", True)

        # Should we include financial terms?
        include_financial = file_cfg.get("include_financial_terms", False)
        financial_field_patterns = file_cfg.get("financial_field_patterns", [])

        # Additional optional fields
        include_optional = file_cfg.get("include_optional_fields", [])

        # Build the field list
        fields_to_include = []
        fields_seen = set()

        # 1. Always include required fields (if flag is set)
        if include_required:
            for fname_lower, fspec in all_fields.items():
                status = fspec.get("Property field status", "")
                if status == "R":
                    real_name = fspec.get("Input Field Name", fname_lower)
                    if real_name not in fields_seen:
                        fields_to_include.append(real_name)
                        fields_seen.add(real_name)

        # 2. Add explicitly listed fields
        for fname in explicit_fields:
            if fname not in fields_seen:
                # Verify it exists in the schema
                fname_lower = fname.lower()
                if fname_lower in all_fields:
                    fields_to_include.append(all_fields[fname_lower].get("Input Field Name", fname))
                    fields_seen.add(all_fields[fname_lower].get("Input Field Name", fname))
                else:
                    logger.warning(f"Field '{fname}' not found in {spec_key} schema for OED {self.oed_version}")
                    fields_to_include.append(fname)
                    fields_seen.add(fname)

        # 3. Add financial fields
        if include_financial:
            for fname_lower, fspec in all_fields.items():
                real_name = fspec.get("Input Field Name", fname_lower)
                rn_lower = real_name.lower()

                # Match by patterns if provided, otherwise include all financial fields
                if financial_field_patterns:
                    if any(p.lower() in rn_lower for p in financial_field_patterns):
                        if real_name not in fields_seen:
                            fields_to_include.append(real_name)
                            fields_seen.add(real_name)
                else:
                    if any(t in rn_lower for t in ["ded", "limit", "tiv", "share", "attach"]):
                        if real_name not in fields_seen:
                            fields_to_include.append(real_name)
                            fields_seen.add(real_name)

        # 4. Add optional fields
        for fname in include_optional:
            fname_lower = fname.lower()
            if fname_lower in all_fields and fname not in fields_seen:
                real_name = all_fields[fname_lower].get("Input Field Name", fname)
                fields_to_include.append(real_name)
                fields_seen.add(real_name)

        # 5. Auto-include conditionally required peril fields
        # When financial fields are present, OED requires the corresponding peril field
        cr_fields = self.schema.get("cr_field", {}).get(spec_key, {})
        peril_fields_needed = set()
        for field_name in fields_to_include:
            if field_name in cr_fields:
                for dep in cr_fields[field_name]:
                    if "peril" in dep.lower() and dep not in fields_seen:
                        peril_fields_needed.add(dep)

        for pf in sorted(peril_fields_needed):
            pf_lower = pf.lower()
            if pf_lower in all_fields:
                real_name = all_fields[pf_lower].get("Input Field Name", pf)
                if real_name not in fields_seen:
                    fields_to_include.append(real_name)
                    fields_seen.add(real_name)
                    logger.debug(f"Auto-included conditionally required field: {real_name}")

        # 6. Always include OEDVersion if configured
        if self.config.get("include_oed_version", True):
            if "OEDVersion" not in fields_seen:
                fields_to_include.append("OEDVersion")
                fields_seen.add("OEDVersion")

        return fields_to_include

    def _generate_file_data(self, file_type: str) -> pd.DataFrame:
        """Generate a DataFrame of test data for a given file type."""
        spec_key = FILE_TYPE_MAP.get(file_type.lower(), file_type)
        all_fields = self.schema["input_fields"].get(spec_key, {})
        file_cfg = self.config.get("files", {}).get(file_type, {})
        num_rows = file_cfg.get("num_rows", 10)

        fields = self._resolve_fields(file_type)
        logger.info(f"Generating {spec_key} with {num_rows} rows and {len(fields)} fields")
        logger.debug(f"Fields: {fields}")

        # Build a lookup from field name to spec
        field_spec_lookup = {}
        for fname_lower, fspec in all_fields.items():
            real_name = fspec.get("Input Field Name", fname_lower)
            field_spec_lookup[real_name] = fspec
            field_spec_lookup[real_name.lower()] = fspec

        data = {f: [] for f in fields}

        for row_idx in range(num_rows):
            # Build row-level context for cross-field consistency
            row_ctx = dict(self.context)

            # For Loc: assign accounts round-robin
            if spec_key == "Loc":
                acc_idx = row_idx % len(self.acc_numbers)
                row_ctx["_current_acc"] = self.acc_numbers[acc_idx]
                row_ctx["_current_port"] = self.acc_to_port[self.acc_numbers[acc_idx]]

            # For Acc: iterate through accounts/policies
            if spec_key == "Acc":
                pol_idx = row_idx % len(self.pol_numbers)
                row_ctx["_current_pol"] = self.pol_numbers[pol_idx]
                row_ctx["_current_acc"] = self.pol_to_acc[self.pol_numbers[pol_idx]]

            for field_name in fields:
                fspec = field_spec_lookup.get(field_name, field_spec_lookup.get(field_name.lower(), {}))

                # Special handling for key fields that need referential integrity
                value = self._generate_with_integrity(
                    field_name, fspec, spec_key, row_idx, row_ctx
                )
                data[field_name].append(value)

            # Track generated loc numbers for RI scope
            if spec_key == "Loc" and "LocNumber" in data:
                loc_num = data["LocNumber"][-1]
                if loc_num not in self.context["loc_numbers"]:
                    self.context["loc_numbers"].append(loc_num)

        return pd.DataFrame(data)

    def _generate_with_integrity(self, field_name: str, field_spec: dict,
                                 file_type: str, row_idx: int, ctx: dict) -> Any:
        """Generate a value maintaining referential integrity across files."""

        # Handle key fields that must be consistent across files
        if file_type == "Loc":
            if field_name == "PortNumber":
                return ctx.get("_current_port", self.port_numbers[0])
            if field_name == "AccNumber":
                return ctx.get("_current_acc", self.acc_numbers[0])

        if file_type == "Acc":
            if field_name == "PolNumber":
                return ctx.get("_current_pol", self.pol_numbers[row_idx % len(self.pol_numbers)])
            if field_name == "AccNumber":
                return ctx.get("_current_acc", self.acc_numbers[row_idx % len(self.acc_numbers)])
            if field_name == "PortNumber":
                acc = ctx.get("_current_acc")
                return self.acc_to_port.get(acc, self.port_numbers[0])

        if file_type == "ReinsScope":
            if field_name == "ReinsNumber":
                if self.context["reins_numbers"]:
                    return self.generator.rng.choice(self.context["reins_numbers"])
                return 1

        # Check for financial field pattern
        fn_lower = field_name.lower()
        if any(p in fn_lower for p in ["ded", "limit"]) and file_type in ("Loc", "Acc"):
            if "type" not in fn_lower and "code" not in fn_lower:
                return self.generator._gen_financial_field(field_name, field_spec, file_type, row_idx, ctx)

        return self.generator.generate_value(field_name, field_spec, file_type, row_idx, ctx)

    def generate(self, output_dir: str, file_format: str = "csv") -> dict[str, str]:
        """Generate all configured OED files.

        Args:
            output_dir: Directory to write output files.
            file_format: Output format ('csv' or 'parquet').

        Returns:
            Dict mapping file type to output file path.
        """
        os.makedirs(output_dir, exist_ok=True)
        output_files = {}
        files_cfg = self.config.get("files", {})

        # Determine generation order (Loc first, then Acc, then RI)
        generation_order = []
        if "loc" in files_cfg:
            generation_order.append("loc")
        if "acc" in files_cfg:
            generation_order.append("acc")
        if "ri_info" in files_cfg:
            generation_order.append("ri_info")
        if "ri_scope" in files_cfg:
            generation_order.append("ri_scope")

        for file_type in generation_order:
            if file_type not in files_cfg:
                continue

            df = self._generate_file_data(file_type)

            # Track reins numbers for scope file
            if file_type == "ri_info" and "ReinsNumber" in df.columns:
                self.context["reins_numbers"] = df["ReinsNumber"].unique().tolist()

            # Determine filename
            spec_key = FILE_TYPE_MAP.get(file_type.lower(), file_type)
            prefix = files_cfg[file_type].get("filename_prefix", "")
            if prefix:
                filename = f"{prefix}_{spec_key}"
            else:
                filename = f"Source{spec_key}OED"

            if file_format == "parquet":
                filepath = os.path.join(output_dir, f"{filename}.parquet")
                df.to_parquet(filepath, index=False)
            else:
                filepath = os.path.join(output_dir, f"{filename}.csv")
                df.to_csv(filepath, index=False)

            output_files[file_type] = filepath
            logger.info(f"  Written {file_type} -> {filepath} ({len(df)} rows, {len(df.columns)} cols)")

        return output_files


# ---------------------------------------------------------------------------
# Config Validation & Defaults
# ---------------------------------------------------------------------------

def validate_config(config: dict) -> list[str]:
    """Validate the generator config and return a list of warnings."""
    warnings = []

    if "oed_version" not in config:
        warnings.append("No 'oed_version' specified, defaulting to '5.0.0'")

    if "files" not in config:
        warnings.append("No 'files' section in config - nothing to generate")
        return warnings

    # Check OED version is available
    oed_version = config.get("oed_version", "5.0.0")
    available = get_available_oed_versions()
    if available and oed_version not in available:
        warnings.append(f"OED version '{oed_version}' not available. Available: {available}")

    for file_type, file_cfg in config["files"].items():
        norm_type = file_type.lower()
        if norm_type not in FILE_TYPE_MAP:
            warnings.append(f"Unknown file type '{file_type}'. Expected: {list(FILE_TYPE_MAP.keys())}")

        if "num_rows" not in file_cfg:
            warnings.append(f"No 'num_rows' for '{file_type}', defaulting to 10")

    return warnings


def create_example_config() -> dict:
    """Create and return an example configuration dict."""
    return {
        "oed_version": "5.0.0",
        "seed": 42,
        "include_oed_version": True,
        "output_format": "csv",
        "global_defaults": {
            "num_portfolios": 1,
            "num_accounts_per_portfolio": 5,
            "num_policies_per_account": 1,
            "peril_code": "WTC",
            "currency": "USD",
            "country_codes": ["US", "GB", "JP", "DE", "FR"],
            "tiv_range": [100000, 10000000],
            "latitude_range": [25.0, 55.0],
            "longitude_range": [-125.0, -65.0],
            "financial_terms": {
                "deductible_range": [1000, 100000],
                "limit_range": [500000, 50000000]
            }
        },
        "files": {
            "loc": {
                "num_rows": 100,
                "include_required": True,
                "include_financial_terms": True,
                "financial_field_patterns": [
                    "LocDed1Building", "LocDedType1Building",
                    "LocLimit1Building", "LocLimitType1Building",
                    "LocDed3Contents", "LocDedType3Contents",
                    "LocLimit3Contents", "LocLimitType3Contents"
                ],
                "fields": [
                    "Latitude", "Longitude", "OccupancyCode", "ConstructionCode",
                    "NumberOfStoreys", "YearBuilt", "BuildingTIV", "OtherTIV",
                    "ContentsTIV", "BITIV"
                ],
                "include_optional_fields": [
                    "StreetAddress", "PostalCode", "City"
                ],
                "fixed_values": {},
                "filename_prefix": "Test"
            },
            "acc": {
                "num_rows": 5,
                "include_required": True,
                "include_financial_terms": True,
                "financial_field_patterns": [
                    "PolDed6All", "PolDedType6All",
                    "PolLimit6All", "PolLimitType6All"
                ],
                "fields": [],
                "fixed_values": {},
                "filename_prefix": "Test"
            },
            "ri_info": {
                "num_rows": 3,
                "include_required": True,
                "include_financial_terms": True,
                "fields": [
                    "ReinsLayerNumber", "ReinsName",
                    "ReinsInceptionDate", "ReinsExpiryDate",
                    "CededPercent", "RiskLimit", "RiskAttachment",
                    "OccLimit", "OccAttachment", "TreatyShare",
                    "RiskLevel"
                ],
                "fixed_values": {
                    "ReinsType": ["QS", "CXL", "PR"]
                },
                "filename_prefix": "Test"
            },
            "ri_scope": {
                "num_rows": 5,
                "include_required": True,
                "fields": [
                    "PortNumber", "AccNumber", "PolNumber", "LocNumber"
                ],
                "fixed_values": {},
                "filename_prefix": "Test"
            }
        }
    }


# ---------------------------------------------------------------------------
# CLI Entry Point
# ---------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(
        description="Generate OED test data files from a JSON configuration.",
        epilog="Example: python oed_generator.py --config config.json --output-dir ./output"
    )
    parser.add_argument(
        "--config", "-c",
        type=str,
        help="Path to JSON configuration file"
    )
    parser.add_argument(
        "--output-dir", "-o",
        type=str,
        default="./oed_output",
        help="Output directory for generated files (default: ./oed_output)"
    )
    parser.add_argument(
        "--format", "-f",
        type=str,
        choices=["csv", "parquet"],
        default=None,
        help="Output file format (overrides config)"
    )
    parser.add_argument(
        "--example-config",
        action="store_true",
        help="Print an example configuration JSON and exit"
    )
    parser.add_argument(
        "--list-versions",
        action="store_true",
        help="List available OED versions and exit"
    )
    parser.add_argument(
        "--list-fields",
        type=str,
        metavar="FILE_TYPE",
        help="List all fields for a given file type (e.g. 'Loc', 'Acc', 'ReinsInfo', 'ReinsScope')"
    )
    parser.add_argument(
        "--oed-version",
        type=str,
        default="5.0.0",
        help="OED version for --list-fields (default: 5.0.0)"
    )
    parser.add_argument(
        "--verbose", "-v",
        action="store_true",
        help="Enable verbose logging"
    )

    args = parser.parse_args()

    # Configure logging
    logging.basicConfig(
        level=logging.DEBUG if args.verbose else logging.INFO,
        format="%(asctime)s [%(levelname)s] %(message)s"
    )

    # Handle utility commands
    if args.list_versions:
        versions = get_available_oed_versions()
        print("Available OED versions:")
        for v in versions:
            print(f"  {v}")
        return

    if args.example_config:
        print(json.dumps(create_example_config(), indent=2))
        return

    if args.list_fields:
        schema = load_oed_schema(args.oed_version)
        file_type = args.list_fields
        fields = schema["input_fields"].get(file_type, {})
        if not fields:
            print(f"No fields found for '{file_type}'. Available: {list(schema['input_fields'].keys())}")
            return
        print(f"Fields for {file_type} (OED {args.oed_version}): {len(fields)} fields")
        print(f"{'Field Name':<40} {'Status':<8} {'Data Type':<20} {'Default':<10}")
        print("-" * 80)
        for fname_lower, fspec in sorted(fields.items()):
            name = fspec.get("Input Field Name", fname_lower)
            status = fspec.get("Property field status", "?")
            dtype = fspec.get("Data Type", "?")
            default = fspec.get("Default", "")
            print(f"{name:<40} {status:<8} {dtype:<20} {default:<10}")
        return

    # Main generation
    if not args.config:
        parser.error("--config is required (or use --example-config to generate one)")

    with open(args.config) as f:
        config = json.load(f)

    # Validate
    warnings = validate_config(config)
    for w in warnings:
        logger.warning(w)

    # Override format if specified
    if args.format:
        config["output_format"] = args.format

    file_format = config.get("output_format", "csv")

    # Generate
    generator = OEDFileGenerator(config)
    logger.info(f"Generating OED {generator.oed_version} test data...")
    output_files = generator.generate(args.output_dir, file_format)

    logger.info(f"\nGenerated {len(output_files)} files:")
    for ft, fp in output_files.items():
        logger.info(f"  {ft}: {fp}")

    # Optionally validate with ods_tools
    if config.get("validate_output", False):
        try:
            import ods_tools
            logger.info("\nValidating generated files with ods_tools...")
            oed_config = {}
            for ft, fp in output_files.items():
                if ft == "loc":
                    oed_config["location"] = fp
                elif ft == "acc":
                    oed_config["account"] = fp
                elif ft == "ri_info":
                    oed_config["ri_info"] = fp
                elif ft == "ri_scope":
                    oed_config["ri_scope"] = fp

            exposure = ods_tools.oed.OedExposure(check_oed=True, **oed_config)
            logger.info("Validation passed!")
        except Exception as e:
            logger.warning(f"Validation failed: {e}")


if __name__ == "__main__":
    main()
