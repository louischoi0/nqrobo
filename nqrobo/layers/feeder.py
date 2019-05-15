from nqrobo.loader.poolsql import poolTsSql,poolSql

class baseFeeder :
    def __init__(self) :
        self.time_series = None
        self.fund_pool = None

    def run(self,**input_dict) :
        input_dict["time_series"] = poolTsSql("ibk","basic").load_fund_pool_ts()
        input_dict["fund_pool"] = poolSql("ibk","basic").load_fund_pool()

        return input_dict