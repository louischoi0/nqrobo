
from functools import reduce

class meta :

    FTYPE_DICT = {
        "stock" : [10, 20],
        "dstock": 10,
        "dbond" : 40,
        "bond" : [40,50],
        "fbond" : 50,
        "fstock" : 20
    }

    def __init__(self) :
        pass

    @staticmethod
    def inv_type_selc(*types) :
        v = [ meta.inv_type_code(t) for t in types ]
        return set(sum(v,[]))

    @staticmethod
    def inv_type_form(*types) :
        return list(meta.inv_type_code(t) for t in types)

    @staticmethod
    def inv_type_code(t) :
        return meta.FTYPE_DICT[t] if type(meta.FTYPE_DICT[t]) == list else [ meta.FTYPE_DICT[t] ]

    @staticmethod
    def assert_idict(idict,*tcls) :
        for c in tcls :
            assert( c in idict )

    @staticmethod
    def fund_set_to_id(fund_set) :
        lam = lambda x,y : x + "!" + y
        return reduce(lam,fund_set)
    
    @staticmethod
    def hash_id_to_fund_set(hid) :
        return hid.split("!")






