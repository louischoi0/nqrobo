from nqrobo.layers.meta import meta
import itertools
import sys
import numpy as np

class baseAllocator :

    def __init__(self) :
        self.fund_pool = None
        self.type_forms = {}
        self.default_form = []

    def add_type_forms(self,robo_id,*form) :
        self.type_forms[robo_id] = meta.inv_type_form(*form)

    def set_default_form(self,*form) :
        self.default_form = meta.inv_type_form(*form)

    def init(self,fund_pool) :
        self.fund_pool = fund_pool

    def run(self,**input_dict) :

        assert("fund_pool" in input_dict)

        self.init(input_dict["fund_pool"])

        robo_id = None if "target_id" not in input_dict else input_dict["target_id"]

        type_form = self.default_form if not robo_id in self.type_forms else self.type_forms[robo_id]
        entries = baseAllocator.fund_kernel(self.fund_pool,type_form)        

        fs = list(itertools.product(*entries))
        input_dict["fund_sets"] = list(filter(lambda x : len(set(x)) == len(x),fs))

        return input_dict

    @staticmethod
    def fund_kernel(fund_pool,form) :
        return list( set(fund_pool[ fund_pool["icode"].isin(f) ].index.values) for f in form )




