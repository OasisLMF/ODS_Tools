# Analysis settings

`analysis_settings.json` controls how a single analysis is run — the number of samples, the
model settings selections, and which outputs (summaries and ORD tables) to produce. It is
validated by `ods_tools` against `analysis_settings_schema.json`. Nested objects (e.g.
`gul_summaries`, and its `ord_output`) are expanded into their own subsections below.

The `ord_output` flags select which result tables to produce; each maps to a table in the
{external+ord:doc}`ORD standard <reference/tables>` (e.g. `ept_full_uncertainty_oep` → the
`EPT`, `elt_sample` → the `SELT`).

```{include} _generated/analysis_settings.md
```
