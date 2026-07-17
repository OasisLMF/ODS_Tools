# Configuration for the ODS_Tools documentation.
#
# ODS_Tools owns the OED loader/validator, the ODTF transformer, and the
# analysis/model settings schemas. Its docs (and executable examples) are
# co-located here and pulled into the aggregated Oasis site via intersphinx.
import os
import sys

sys.path.insert(0, os.path.abspath("_ext"))

project = "ODS Tools"
copyright = "Oasis Loss Modelling Framework"
author = "OasisLMF"

extensions = [
    "myst_nb",            # Markdown (MyST) + executable notebooks
    "sphinx_design",
    "sphinx_copybutton",
    "gen_settings_reference",  # build-time settings-schema reference from ods_tools/data/*.json
]

source_suffix = {
    ".rst": "restructuredtext",
    ".md": "myst-nb",
    ".ipynb": "myst-nb",
}

myst_enable_extensions = ["colon_fence", "deflist", "substitution", "tasklist"]
myst_heading_anchors = 3

# -- Executable notebooks (myst-nb) -----------------------------------------
# These examples exercise the ods_tools LIBRARY (load/validate/transform OED) —
# light, fast operations — so they DO execute at docs-build (the build doubles as
# a smoke test). stderr (tqdm progress bars) is dropped for clean output.
nb_execution_mode = "cache"
nb_execution_raise_on_error = True
nb_execution_timeout = 120
nb_output_stderr = "remove"

exclude_patterns = ["_build", "Thumbs.db", ".DS_Store",
                    # _generated/*.md are include-only fragments, not standalone documents
                    "reference/_generated/**"]

html_theme = "furo"
html_title = "ODS Tools"


# -- Cross-component links (intersphinx, aggregated site) --------------------
# The GenerateDocs orchestrator sets OASIS_INTERSPHINX_MAP (JSON) to point cross-references at
# the other components' built inventories; standalone builds add nothing. Use explicit roles,
# e.g. {external+ord:doc}`reference/tables` or :external+oed:ref:`some-label`.
import json as _ix_json, os as _ix_os
if "sphinx.ext.intersphinx" not in extensions:
    extensions = list(extensions) + ["sphinx.ext.intersphinx"]
try:
    intersphinx_mapping
except NameError:
    intersphinx_mapping = {}
intersphinx_mapping.update({
    _k: (_v[0], _v[1])
    for _k, _v in _ix_json.loads(_ix_os.environ.get("OASIS_INTERSPHINX_MAP", "{}")).items()
})
