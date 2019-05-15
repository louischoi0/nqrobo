from nqrobo.loader.sql_base import netAssetSql,schema
from qrobo.utils.path import IBK_BENCH_MODEL
from nqrobo.loader.sql_base import *

class ibkBenchNetAssetSchema(schema) :
    def __init__(self) :
        columns = generate_mp_column_name(5)
        dtype = generate_mp_type(5)

        super(ibkBenchNetAssetSchema,self).__init__(columns,dtype)

class ibkBenchNetAssetSql(netAssetSql) :
    def __init__(self) :
        super(ibkBenchNetAssetSql,self).__init__("ibk","bench",ibkBenchNetAssetSchema())
        self.target_dir = IBK_BENCH_MODEL
        self.mp_file_format = "mp_fund_"
        self.robo_targets = self.generate_robo_ids() 
        self.target_tables = list(map(lambda x : "ibk_bench_net_asset_{}".format(x),self.robo_targets))

    def generate_robo_ids(self) :
        robo_id_2 = ["1","3","4","5"]
        ids = list(map(lambda z : list(map(lambda y : list(map(lambda x : "{}{}{}5".format(x,y,z), range(1,5))),robo_id_2)),range(2)))
        ids = sum(ids,[])
        return sum(ids,[])

if __name__ == "__main__" :
    ibkn =  ibkBenchNetAssetSql()
    print(ibkn.robo_targets)