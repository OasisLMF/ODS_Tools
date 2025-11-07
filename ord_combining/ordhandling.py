import pandas as pd

# Handling + merging ELT files

def merge_melt(gpqt, melt):
    _melt = melt[['EventId', 'MeanLoss', 'SDLoss', 'MaxLoss']]
    merged = gpqt.merge(_melt, on='EventId', how='outer')
    merged['not_merged'] = merged[['MeanLoss', 'SDLoss', 'MaxLoss']].isna().any(axis='columns')

    return merged

def merge_qelt(gpqt, qelt):
    _qelt = qelt[["EventId", "LTQuantile", "QuantileLoss"]]
    merged = gpqt.merge(_qelt, on='EventId', how='outer')
    merged['not_merged'] = merged[["LTQuantile", "QuantileLoss"]].isna().any(axis='columns')
    return merged

def merge_selt(gpqt, selt):
    _selt = selt[['EventId','SampleId','SampleLoss','ImpactedExposure']]
    merged = gpqt.merge(_selt, on='EventId', how='outer')
    merged['not_merged'] = merged[["Quantile", "SampleLoss"]].isna().any(axis='columns')
    return merged

def read_melt(path):
    return pd.read_csv(path).query('SampleType == 2')

def read_qelt(path):
    return pd.read_csv(path).rename(columns={"Loss": "QuantileLoss",
                                             "Quantile": "LTQuantile"})

def read_selt(path):
    # Remove negative sampleids
    return pd.read_csv(path).query('SampleId > 0').rename(columns={"Loss": "SampleLoss"})

