# ---
# jupyter:
#   jupytext:
#     formats: ipynb,py:percent
#     text_representation:
#       extension: .py
#       format_name: percent
#       format_version: '1.3'
#       jupytext_version: 1.18.1
#   kernelspec:
#     display_name: Python 3 (ipykernel)
#     language: python
#     name: python3
# ---

# %% [markdown]
# # Combining Results in ORD
#
# This notebook provides a proof of concept example for combining catastrophe
# loss model results in the Open Results Data (ORD) format. We follow the
# methodology outlined in *Combining_results_in_ORD_v1.1.pdf*.
#
# This notebook is split into the workflow sequence as follows:
#
# 1. Load and Group
# 2. Period Sampling
# 3. Loss Sampling
# 4. Output Preparation

# %%
# make sure relative imports work
# todo: remove after packaging
import os
import sys
module_path = os.path.abspath(os.path.join('..'))
if module_path not in sys.path:
    sys.path.append(module_path)


# %% [markdown]
# ## 1. Load and Group
# ### Creating Analysis and OutputSet
# In this section we create the objects required prior to grouping, namely:
# - Analysis table which contains the meta data from the analyses
# - OutputSet table which contains references to the ORD results.
#
# The `Analysis` and `OutputSet` dataclasses are defined in `common.py`.

# %%
from ord_combining.common import Analysis, OutputSet

# %%
?Analysis
# %%
?OutputSet
