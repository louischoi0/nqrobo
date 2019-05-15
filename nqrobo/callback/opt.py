import nqrobo.callback.functions as cb
import numpy as np

from nqrobo.obj.ga import GA


def mom_ga_a(time_series,fund_info,supervisor=None,delta=30) :
    num_funds = time_series.columns.size
    weights = cb.mom(delta,time_series)
    score = sum(weights)

    op = -1 if min(weights) < 0 else 1

    weights += min(weights) * op * 1.2
    weights /= sum(weights)

    return weights, score


def mom_ga(ts,fi,consts) :

    def obj(w) :
        s = cb.mom(30,ts)
        op = -1 if min(s) else 1
        s += min(s) * op * 2
        return sum(s*w) 

    return GA(ts,obj,consts).run()