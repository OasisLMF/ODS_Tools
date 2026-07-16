# Configuration for the ODS_Tools documentation.
#
# ODS_Tools owns the OED loader/validator, the ODTF transformer, and the
# analysis/model settings schemas. Its docs (and executable examples) are
# co-located here and pulled into the aggregated Oasis site via intersphinx.

project = "ODS Tools"
copyright = "Oasis Loss Modelling Framework"
author = "OasisLMF"

extensions = [
    "myst_nb",            # Markdown (MyST) + executable notebooks
    "sphinx_design",
    "sphinx_copybutton",
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

exclude_patterns = ["_build", "Thumbs.db", ".DS_Store"]

html_theme = "furo"
html_title = "ODS Tools"
