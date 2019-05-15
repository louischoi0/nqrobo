import numpy as np
from nqrobo.layers.meta import meta

class riskConstraint :
    def __init__(self,thres,targets,max_or_min) :
        self.thres = thres
        self.op = -1 if max_or_min == "max" else 1
        self.targets = targets

    def init_const(self,risks) :
        self.risks = risks
        self.obj = lambda weights : self.op * ( np.sum( weights * self.risks ) - self.thres ) >= 0 

    def check(self,weights) :
        return self.obj(weights)

class weightConstraint :
    def __init__(self,thres,targets,max_or_min) :
        self.max_or_min = max_or_min

        self.targets = set(meta.inv_type_selc(targets))
        self.thres = thres
        self.op = -1 if self.max_or_min == "max" else 1
        
    def init_const(self,types) :
        self.ipool = types
        obj = lambda x , idx : idx if set([x]).issubset(self.targets) else -1

        tidx =  list(map(obj,self.ipool,range(len(types))))
        self.tidx = np.array(list(filter( lambda x : not x == -1 , tidx ))) if not self.targets == "all" else range(len(types))

        #뽑힌 펀드풀에 타겟 펀드 풀이 없을시 항상 True 반환 해서 계산 로직 최적화
        if len(self.tidx) == 0 : 
            self.obj = lambda weights : True

        else :
            self.obj = lambda weights : self.op * ( np.sum( weights[self.tidx] ) - self.thres ) >= 0

    def check(self,weights) :
        return self.obj(weights)

