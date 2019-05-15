import nqrobo.callback.functions as cb
import nqrobo.callback.opt as op

from nqrobo.layers.meta import meta

import nqrobo.layers.constraint as const 
import nqrobo.layers.const_generator as cgen 

from nqrobo.layers.meta import meta

import numpy as np

{ "robo_id" : "1104", "type" : "max" , "target" : "dstock", "value" : 0.5 }
{ "type" : "min" , "target" : "dstock", "value" : 0.5 }

class baseSupervisor :

    base_bond_min_gen = lambda robo_id : 0.2 + ( int(robo_id[1]) * 0.1 )
    base_bond_max_gen = lambda robo_id : 0.4 - ( int(robo_id[1]) * 0.1 )

    base_stock_min_gen = lambda robo_id : 0.4 - ( int(robo_id[1]) * 0.1 )
    base_stock_max_gen = lambda robo_id : 0.2 + ( int(robo_id[1]) * 0.1 )

    def __init__(self,**input_dict) :
        self.constraints = {}

    def init_const(self,robo_ids) :
        self.add_wconstraints_fg( cgen.baseBondMinGenerator(),robo_ids)
        #self.add_wconstraints_fg( cgen.baseBondMaxGenerator(),robo_ids)
        #self.add_rconstraints_fg( cgen.baseRiskMinGenerator(),robo_ids)
        #self.add_rconstraints_fg( cgen.baseRiskMaxGenerator(),robo_ids)

    def add_wconstraints_fg(self,generator,robo_ids) :
        for ri in robo_ids :
            thres = generator.get_thres(ri)
            c = const.weightConstraint(thres,generator.target,generator.mm)

            self.add_constraints(ri,c)

    def add_rconstraints_fg(self,generator,robo_ids) :
        for ri in robo_ids :
            thres = generator.get_thres(ri)
            c = const.riskConstraint(thres,generator.target,generator.mm)
            
            self.add_constraints(ri,c)

    def add_constraints(self,robo_id,const) :
        if robo_id in self.constraints :
            self.constraints[robo_id].append(const)
        else :
            self.constraints[robo_id] = [ const ]

    def export_constraints(self,robo_id,types,risks) :

        def sub(c) :
            if type(c) == const.weightConstraint :
                c.init_const(types)

            elif type(c) == const.riskConstraint :
                c.init_const(risks)
            
            else :
                assert(False)

            return c.obj

        return list(map(lambda x : sub(x),self.constraints[robo_id]))

    def get_const(self,robo_id) :
        return self.constraints[robo_id]

    def check(self,weights) :
        pass

def test_base_sup() :
    robo_ids = [ "1104","1204","1304" ]
    sup = baseSupervisor()
    sup.init_const(robo_ids)

    const = sup.get_const("1104") 

    risks = np.array([ 3,3,4,3])
    w = np.array([0.25,0.25,0.25,0.25])

    risk_score = np.sum(risks*w)

    rc_lower = const[2]

    rc_lower.init_const(risks)
    b = rc_lower.check(w)

    rc_upper = const[3]
    rc_upper.init_const(risks)

    print(risk_score)
    print(rc_upper.thres)
    print(b)
    

if __name__ == "__main__" :
    test_base_sup()

class scoreAccumulator :
    def __init__(self,eval_func="mom") :
        self.eval_func = eval_func

    def run(self,**input_dict) :
        pass

class baseOptimizer :

    def __init__(self,eval_func="mom_ga",**input_dict) :
        self.time_series = None
        self.fund_pool = None
        self.eval_func = eval_func
        self.fund_sets = None
        self.supv = baseSupervisor()

        self.constraints = None

    def set_eval_func(self,f) :
        self.eval_func = f

    def run(self,**input_dict) :
        target_id = input_dict["target_id"]
        self.supv.init_const(input_dict["robo_ids"])
        self.constraints = self.supv.get_const(input_dict["target_id"])

        from nqrobo.loader.class_builder import idictBinder

        meta.assert_idict(input_dict,"time_series","fund_pool","fund_sets")
        idictBinder.bind(self,input_dict,"time_series","fund_pool","fund_sets","target_id")

        self.fund_sets = input_dict["fund_sets"]

        wd,sd = self.eval_sets(self.fund_sets,target_id)

        input_dict["weight_sets"] = wd
        input_dict["score_sets"] = sd

        return input_dict

    def eval_sets(self,fund_sets,robo_id) :
        weight_dict = {}
        score_dict = {} 

        for fset in self.fund_sets[:100] :

            stime_series = self.time_series.loc[:,fset]
            sfund_pool = self.fund_pool.loc[fset,:]

            types = sfund_pool["icode"].values
            risks = sfund_pool["risk"].values
            print(sfund_pool)
            consts = self.supv.export_constraints(robo_id,types,risks)

            weights , score = self.eval_fund_set(stime_series,sfund_pool,consts)
            fkey = meta.fund_set_to_id(fset)

            weight_dict[fkey] = weights
            score_dict[fkey] = score

        return weight_dict,score_dict 

    def eval_fund_set(self,stime_series,sfund_pool,consts) :
        eval_func = getattr(op,self.eval_func)
        return eval_func(stime_series,sfund_pool,consts)




