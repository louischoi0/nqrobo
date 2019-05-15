
import pandas as pd
import copy
import numpy as np

from nqrobo.layers.meta import meta
import nqrobo.callback.functions as cb
import sys

from nqrobo.obj.objects import ibkBasicObject
from nqrobo.loader.class_builder import classBuilder
from nqrobo.loader.poolsql import poolSql,poolTsSql
from nqrobo.layers.allocator import baseAllocator
from nqrobo.layers.screener import baseScreener


def test_num_selection_dict(bs) :

    bs.add_fund_types("dstock","stock","bond","fstock","dbond")
    bs.set_num_selection({"dstock":2,"stock":1,"bond":2,"dbond":1})

    assert( bs.get_type_num_selection("stock") == 1 )
    assert( bs.get_type_num_selection("dstock") == 2 )
    assert( bs.get_type_num_selection("fstock") == 1 )
    assert( bs.get_type_num_selection("dbond") == 1 )
    assert( bs.get_type_num_selection("bond") == 2 )


def test_scr(bs,input_dict) :

    bs.add_filter("stock", { "type" : "rvn", "thres" :  0.4 , "delta" : 30 , "eval" : "rel"  })
    bs.add_filter("dstock", { "type" : "rvn", "thres" :  0.4 , "delta" : 30 , "eval" : "rel"  })
    bs.add_filter("bond", { "type" : "rvn", "thres" :  0.4 , "delta" : 30 , "eval" : "rel"  })

    res = bs.run(**input_dict)

    print(res["fund_pool"])
    print(res["fund_pool"].index.size)

    return res

def test_alloc(allocator,idict) :
    allocator.add_type_forms("1104","stock","dstock","bond","bond")
    allocator.set_default_form("bond","stock","stock","dstock")

    df = allocator.run(**idict)

    print(len(list(df["fund_sets"])))

def test_obj() :

    input_dict = {
        "target_id" : "1204",
        "robo_ids" : [ "1104","1204","1304","1404" ]
    }

    obj = ibkBasicObject()
    idict = obj.run_id("1104",**input_dict)
    obj.test()


if __name__ == "__main__" :
    test_obj()