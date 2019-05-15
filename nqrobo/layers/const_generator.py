

class thresGenerator :
    def __init__(self,obj) :
        self.obj = obj
        self.targets = []

    def get_thres(self,robo_id) :
        return self.obj(robo_id)

class baseRiskMinGenerator(thresGenerator) :
    def __init__(self) :
        obj = lambda x : 3.0 - (int(x[1]) * 0.1)
        self.mm = "min"
        self.target = "all"
        super(baseRiskMinGenerator,self).__init__(obj)

class baseRiskMaxGenerator(thresGenerator) :
    def __init__(self) :
        obj = lambda x : 3.0 + (int(x[1]) * 0.1)
        self.mm = "max"
        self.target = "all"
        super(baseRiskMaxGenerator,self).__init__(obj)

class baseBondMinGenerator(thresGenerator) :
    def __init__(self):
        obj = lambda x : 0.2 + (int(x[1]) * 0.1)
        self.target = "bond"
        self.mm = "min"
        super(baseBondMinGenerator,self).__init__(obj)

class baseBondMaxGenerator(thresGenerator) :
    def __init__(self):
        obj = lambda x : 0.2 - (int(x[1]) * 0.1)
        self.target = "bond"
        self.mm = "max"
        super(baseBondMaxGenerator,self).__init__(obj)

class baseStockMinGenerator(thresGenerator) :
    def __init__(self):
        obj = lambda x : 0.2 + (int(x[1]) * 0.1)
        self.target = "bond"
        self.mm = "min"
        super(baseStockMinGenerator,self).__init__(obj)

class baseStockMaxGenerator(thresGenerator) :
    def __init__(self):
        obj = lambda x : 0.2 - (int(x[1]) * 0.1)
        self.target = "bond"
        self.mm = "max"
        super(baseStockMaxGenerator,self).__init__(obj)

class generatorBuilder :

    TDICT = {
        "max" : {
            "risk" : baseRiskMaxGenerator,
            "bond" : baseBondMinGenerator,
            "stock" : baseStockMaxGenerator
        },

        "min"  : {
            "risk" : baseRiskMaxGenerator,
            "bond" : baseBondMinGenerator,
            "stock" : baseStockMinGenerator
            
        }
    }

    def __init__(self) :
        pass

    @staticmethod 
    def get(mm,t) :
        return generatorBuilder.TDICT[mm][t]