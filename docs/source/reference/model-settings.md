# Model settings

`model_settings.json` describes a model's runtime capabilities, lookup configuration and
defaults. It is supplied by the model and validated by `ods_tools` against
`model_settings_schema.json`. Nested objects (e.g. `model_settings`, `lookup_settings`) are
expanded into their own subsections below.

```{include} _generated/model_settings.md
```
