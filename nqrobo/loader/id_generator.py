import itertools
from nqrobo.loader.sql_base import get_connection
import sys
import copy

class roboIdGenerator :
    def __init__(self,rlist) :
        self.robo_ids = self.get_robo_ids(rlist) 

    def get_robo_ids(self,rlist) :
        strf = "{}" * len(rlist)
        ids = itertools.product(*rlist)

        return list(map(lambda x : strf.format(*x), ids))

class bnkbProBasicIdGenerator(roboIdGenerator) :
    def __init__(self) :
        r0 = [ "8" ]
        r1 = [ "1","2","3","4","5"]
        r2 = ["0"]
        r3 = ["5"] 
        rlist = [r0,r1,r2,r3]

        self.model_nums = [ 1 ]

        super(bnkbProBasicIdGenerator,self).__init__(rlist)

class bnkbBasicIdGenerator(roboIdGenerator) :
    def __init__(self) :
        r0 = [ "1" ]
        r1 = [ "1","2","3","4","5"]
        r2 = ["0","1"]
        r3 = ["5"] 
        rlist = [r0,r1,r2,r3]

        self.model_nums = list(range(1,10))

        super(bnkbBasicIdGenerator,self).__init__(rlist)

class bnkbBenchIdGenerator(roboIdGenerator) :
    def __init__(self) :
        r0 = [ "4" ]
        r1 = [ "1","2","3","4","5"]
        r2 = ["0","1"]
        r3 = ["5"] 
        rlist = [r0,r1,r2,r3]

        self.model_nums = [1]

        super(bnkbBenchIdGenerator,self).__init__(rlist)

class bnkbRetireIdGenerator(roboIdGenerator) :
    def __init__(self) :
        r0 = [ "2" ]
        r1 = [ "1","2","3","4","5"]
        r2 = ["0"]
        r3 = ["5"] 
        rlist = [r0,r1,r2,r3]
        self.model_nums = list(range(1,10))

        super(bnkbRetireIdGenerator,self).__init__(rlist)

class bnkkBenchIdGenerator(roboIdGenerator) :
    def __init__(self) :
        r0 = [ "4" ]
        r1 = [ "1","2","3","4","5"]
        r2 = ["0","1"]
        r3 = ["5"] 
        rlist = [r0,r1,r2,r3]

        self.model_nums = [ 1 ]
        super(bnkkBenchIdGenerator,self).__init__(rlist)

class bnkkProBasicIdGenerator(roboIdGenerator) :
    def __init__(self) :
        r0 = [ "8" ]
        r1 = [ "1","2","3","4","5"]
        r2 = ["0"]
        r3 = ["5"] 
        rlist = [r0,r1,r2,r3]

        self.model_nums = [ 1 ]
        super(bnkkProBasicIdGenerator,self).__init__(rlist)

class bnkkBasicIdGenerator(roboIdGenerator) :
    def __init__(self) :
        r0 = [ "1" ]
        r1 = [ "1","2","3","4","5"]
        r2 = ["0","1"]
        r3 = ["5"] 
        rlist = [r0,r1,r2,r3]
        self.model_nums = list(range(1,10))

        super(bnkkBasicIdGenerator,self).__init__(rlist)

class bnkkProRetireIdGenerator(roboIdGenerator) :
    def __init__(self) :
        r0 = [ "9" ]
        r1 = [ "1","2","3","4","5"]
        r2 = ["0"]
        r3 = ["5"] 
        rlist = [r0,r1,r2,r3]

        self.model_nums = [ 1 ]
        super(bnkkProRetireIdGenerator,self).__init__(rlist) 

class bnkkRetireIdGenerator(roboIdGenerator) :
    def __init__(self) :
        r0 = [ "2" ]
        r1 = [ "1","2","3","4","5"]
        r2 = ["0"]
        r3 = ["5"] 
        rlist = [r0,r1,r2,r3]
        self.model_nums = list(range(1,10))

        super(bnkkRetireIdGenerator,self).__init__(rlist) 

class ibkWinIdGenerator(roboIdGenerator) :
    def __init__(self) :
        r0 = [ "4" ]
        r1 = [ "1","2","3","4","5"]
        r2 = ["0","1"]
        r3 = ["5"] 
        rlist = [r0,r1,r2,r3]

        self.model_nums = [ 1 ]
        super(ibkWinIdGenerator,self).__init__(rlist)

class ibkIsaIdGenerator(roboIdGenerator) :
    def __init__(self) :
        r0 = [ "5" ]
        r1 = [ "1","2","3","4","5","6","7"]
        r2 = ["0"]
        r3 = ["9"] 
        rlist = [r0,r1,r2,r3]

        self.model_nums = [ 1 ]
        super(ibkIsaIdGenerator,self).__init__(rlist)

class ibkBasicIdGenerator(roboIdGenerator) : 
    def __init__(self) :
        r0 = [ "1" ]
        r1 = [ "1","3","4","5"]
        r2 = ["0","1"]
        r3 = ["3","4","5"] 
        rlist = [r0,r1,r2,r3]
        self.model_nums = list(range(1,10))

        super(ibkBasicIdGenerator,self).__init__(rlist)
        self.pbasic = ["3105","3305","3405","3505"]
        self.robo_ids += self.pbasic 

class ibkRetireIdGenerator(roboIdGenerator):
    def __init__(self) :
        r0 = [ "2" ]
        r1 = [ "1","3","4","5"]
        r2 = ["0","1"]
        r3 = ["3","4","5"] 
        rlist = [r0,r1,r2,r3] 
        self.model_nums = [1,2,3,4,5,6]

        super(ibkRetireIdGenerator,self).__init__(rlist)

class ibkRetireInternetIdGenerator(ibkRetireIdGenerator):
    def __init__(self) :
        super(ibkRetireInternetIdGenerator,self).__init__()
        self.model_nums = [1,2,3,4,5,6]

class ibkMarketIdGenerator(roboIdGenerator):
    def __init__(self) :
        self.robo_ids = ["2309"]
        self.model_nums = [ 1 ]

class ibkBenchIdGenerator(roboIdGenerator) :
    def __init__(self) :
        r = [ ["1"],["1","3","4","5"],["0","1"],["5"] ]
        ids = self.get_robo_ids(r)

        r = [ ["3"],["1","3","4","5"],["0"],["5"] ]
        _ids = self.get_robo_ids(r)

        self.basicp_robo_ids = copy.deepcopy(_ids)
        ids += _ids

        r = [ ["2"],["1","3","4","5"],["0","1"],["5"] ]
        _ids = self.get_robo_ids(r)
        ids += _ids

        r = [ ["4"],["1","2","3","4","5"],["0","1"],["5"] ]
        _ids = self.get_robo_ids(r)

        self.win_robo_ids = copy.deepcopy(_ids)
        self.robo_ids = ids + _ids
        self.model_nums = [ 1 ]

BNKB = ["Basic","Retire","Bench","ProBasic"]
BNKK = ["Basic","Retire","Bench","ProBasic","ProRetire"]
