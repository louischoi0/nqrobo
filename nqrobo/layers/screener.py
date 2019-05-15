import pandas as pd
import copy
import numpy as np

from nqrobo.layers.meta import meta
import nqrobo.callback.functions as cb
import sys

from nqrobo.loader.poolsql import poolSql,poolTsSql
from nqrobo.layers.allocator import baseAllocator

import functools

OP_DPRINT = True

def dprint(v) :
    if OP_DPRINT:
        print(v)

class baseScreener :

    def __init__(self,eval_func="mom") :
        self.eval_func = eval_func
        self.fund_pool = None
        self.time_series = None

        self.filter_conditions = {}
        self.res_pool = None
        self.use_types = [] 
        self.risk_cache = {}
        self.num_selection = {}
        self.delta = 30

    def set_eval_func(self,func) :
        self.eval_func = func 

    def set_num_selection(self,ndict) :
        self.num_selection = ndict

    def get_type_num_selection(self,itype) :
        inv_types = list(x for x in self.num_selection) 
        target = list(filter(lambda x : set(meta.inv_type_code(itype)).issubset(set(meta.inv_type_code(x))) , inv_types ))

        if len(target) == 1 :
            return self.num_selection [ target[0] ]

        else :
            target = list(filter(lambda x : len(meta.inv_type_code(x)) == 1 , target ))
            return self.num_selection [ target[0] ]

    def init(self,time_series,fund_pool) :
        self.time_series = time_series.fillna(method="bfill").dropna(axis=1)
        self.init_fund_pool(fund_pool)

    def init_fund_pool(self,fund_pool) :
        self.fund_pool = fund_pool
        type_selector = meta.inv_type_selc(*self.use_types)
        self.fund_pool = self.fund_pool [ self.fund_pool["icode"].isin(type_selector) ]

        # 가격데이터가 있는거만 추림
        self.fund_pool = baseScreener.getsubpool(self.fund_pool,self.time_series.columns.values)

    @staticmethod
    def getsubpool(pool,funds) :
        funds_exists = map(lambda x : x in funds,pool.index.values)
        return pool.loc[funds_exists,:]

    def run(self,**input_dict) :
        ts = input_dict["time_series"]
        fp = input_dict["fund_pool"]

        self.init(ts,fp)
        self.filter()
        res = self.select_funds()
        
        input_dict["fund_pool"] = res
        input_dict["use_types"] = self.use_types
        input_dict["time_series"] = self.time_series

        return input_dict

    def filter(self) :
        for fund_type in self.filter_conditions :

            type_selector = meta.inv_type_code(fund_type)
            conditions = self.filter_conditions[fund_type]

            funds = self.fund_pool[ self.fund_pool["icode"].isin(type_selector) ].index.values

            if (len(funds) == 0 ):
                continue

            stime_series = self.time_series.loc[:,funds]
            filtered = self.filter_subroutine(stime_series,conditions)

            # 2중 drop시 발생되는 error 막기 위한 로직
            filtered = list(filter(lambda x : x in self.fund_pool.index.values,filtered))
            self.fund_pool = self.fund_pool.drop(filtered)

    def cache_risk_info(self) :
        
        for t in self.use_types :
            icode_set = meta.inv_type_code(t)
            funds = self.fund_pool[ self.fund_pool["icode"].isin(icode_set) ]

            risks = set(funds["risk"].values)
            self.risk_cache[t] = risks


    def select_funds(self) :
        self.cache_risk_info()
        res = pd.DataFrame(columns=self.fund_pool.columns.values)

        for inv_type in self.risk_cache :
            risks = self.risk_cache[inv_type]

            for risk in risks :
                selected = self.select_funds_for_type_and_risk(inv_type,risk)
                selected = list(filter(lambda x : x not in res.index,selected))

                res = pd.concat([res, self.fund_pool.loc[selected,:] ],axis=0,sort=True)

        return res


    def getsubpooltr(self,itype,risk) :
        risk = [ risk ] if not type(risk) == list else risk
        itype = meta.inv_type_selc(itype)

        f = self.fund_pool [ self.fund_pool["risk"].isin(risk) ]
        return f[ f["icode"].isin(itype) ]


    def select_funds_for_type_and_risk(self,itype,risks) :
        fund_pool = self.getsubpooltr(itype,risks)
        funds = fund_pool.index.values
        stime_series = self.time_series.loc[:,funds]

        assert( len(funds) == stime_series.columns.size )

        eval_func = getattr(cb,self.eval_func)
        scores = eval_func(self.delta,stime_series)

        idx = np.argsort(scores)
        num_select = self.get_type_num_selection(itype)
        idx = idx[-num_select:]

        assert( num_select >= len(funds[idx]) )

        return funds[idx]

    def filter_subroutine(self,time_series,conds):
        res = []

        for cond in conds :
            r = baseScreener.eval_condtion(time_series,cond) 

            res.extend(r)

        return set(res)

    @staticmethod
    def eval_condtion(time_series,cond) :

        func = getattr(cb,cond["type"])
        funds = time_series.columns.values
        thres = cond["thres"] 
        delta = cond["delta"]

        val = func(delta,time_series)

        if cond["eval"] == "rel" :
            val = cb.to_rel_score(val)

        idx, = np.where( val < thres )

        return funds[idx]

    def add_filter(self,target_type,condition) :
        if target_type in self.filter_conditions :
            self.filter_conditions[target_type].append(condition)
        else : 
            self.filter_conditions[target_type] = [ condition ]

    def add_fund_types(self,*types) :
        self.use_types.extend(types)