import numpy as np

def rvn(delta,time_series) :
    ts = time_series.iloc[-delta:,:]
    return (ts.iloc[-1,:].values - ts.iloc[0,:].values) / ts.iloc[0,:].values

def to_rel_score(scores) :
    return np.argsort(scores) / len(scores)

def mom(delta,time_series) :
    return rvn(delta,time_series)

