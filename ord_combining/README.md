# Combining ORD Demo

This folder contains the scripts necessary to run a demo of the method described in
*Combining_results_in_ORD_v1.1.pdf*.

We run this protocol on an artificial exposure set (stored in `piwind-ord/`) which runs
with the PiWind model. The `full/` directory contains the full test exposure set for
PiWind and the `split/` exposure set is the full exposure set split in two randomly. We
run the combining_ord procedure on the 10 location version of the exposure set.

> Note this directory was prepared using `jupytext` and so the notebooks `{filename}.ipynb`
> are paired with a python script file `{filename}.py`.

There are two notebooks in this directory.

1) `combining_ord.ipynb` - Performs the full procedure for combining the results from the
`split/` exposure set.
    - Example outputs from running using `QELT` and `SELT` are stored in the `combined_ord-qelt` and `combining_ord-selt` directories respectively.
2. `OutputComparison.ipynb` - Compare the output of `combining_ord.ipynb` with the
    output from running PiWind on the `full/` exposure set.
