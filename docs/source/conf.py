# Configuration for the ODS_Tools documentation.
#
# ODS_Tools owns the OED loader/validator, the ODTF transformer, and the
# analysis/model settings schemas. Its docs (and executable examples) are
# co-located here and pulled into the aggregated Oasis site via intersphinx.
import json
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
# the other components' built inventories. Use explicit roles, e.g.
# {external+ord:doc}`reference/tables` or :external+oed:ref:`some-label`.
extensions.append("sphinx.ext.intersphinx")
_oasis_inventories = json.loads(os.environ.get("OASIS_INTERSPHINX_MAP", "{}"))
intersphinx_mapping = {name: (base, inv) for name, (base, inv) in _oasis_inventories.items()}
if not _oasis_inventories:
    # Standalone build: the sibling components' inventories don't exist yet, so an
    # {external+...} reference cannot resolve and warning about it says nothing useful.
    # Only the orchestrated build can actually check these, so it stays strict.
    suppress_warnings = ["intersphinx.external"]

# -- Oasis shared branding (logo, palette, GitHub footer) -------------------
html_static_path = ["_static"]
html_theme_options = {
    "light_logo": "OASIS_LMF_COLOUR.png",
    "dark_logo": "OASIS_LMF_WHITE.png",
    "light_css_variables": {
        "color-brand-primary": "#862633",
        "color-brand-content": "#d22630",
        "font-stack": "Raleway, sans-serif",
    },
    "dark_css_variables": {
        "color-brand-primary": "#e2919b",
        "color-brand-content": "#ef8b93",
    },
    # GitHub link — Furo's conventional spot is the footer icons (bottom of every page)
    "footer_icons": [{
        "name": "GitHub", "url": "https://github.com/OasisLMF", "class": "",
        "html": '<svg stroke="currentColor" fill="currentColor" stroke-width="0" viewBox="0 0 16 16"><path fill-rule="evenodd" d="M8 0C3.58 0 0 3.58 0 8c0 3.54 2.29 6.53 5.47 7.59.4.07.55-.17.55-.38 0-.19-.01-.82-.01-1.49-2.01.37-2.53-.49-2.69-.94-.09-.23-.48-.94-.82-1.13-.28-.15-.68-.52-.01-.53.63-.01 1.08.58 1.23.82.72 1.21 1.87.87 2.33.66.07-.52.28-.87.51-1.07-1.78-.2-3.64-.89-3.64-3.95 0-.87.31-1.59.82-2.15-.08-.2-.36-1.02.08-2.12 0 0 .67-.21 2.2.82.64-.18 1.32-.27 2-.27.68 0 1.36.09 2 .27 1.53-1.04 2.2-.82 2.2-.82.44 1.1.16 1.92.08 2.12.51.56.82 1.27.82 2.15 0 3.07-1.87 3.75-3.65 3.95.29.25.54.73.54 1.48 0 1.07-.01 1.93-.01 2.2 0 .21.15.46.55.38A8.013 8.013 0 0 0 16 8c0-4.42-3.58-8-8-8z"></path></svg>',
    }],
}
html_css_files = ["https://fonts.googleapis.com/css?family=Raleway"]
