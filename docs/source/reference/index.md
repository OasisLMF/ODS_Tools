# Reference

ODS_Tools bundles and validates the **settings schemas** used across the Oasis toolchain.
These pages are generated at build time from the JSON Schemas in `ods_tools/data/` — edit
the schema, not the generated tables.

- **{doc}`model-settings`** — a model's capabilities and defaults (`model_settings.json`),
  supplied by the model.
- **{doc}`analysis-settings`** — how a single analysis is run and what outputs it produces
  (`analysis_settings.json`), supplied per run.
- **{doc}`combine-settings`** — configuration for combining ORD results.

```{toctree}
:maxdepth: 2

model-settings
analysis-settings
combine-settings
```
