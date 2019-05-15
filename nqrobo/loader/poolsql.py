import pandas as pd
from nqrobo.loader.sql_base import get_connection

class poolSql :
    def __init__(self,bank,service) :
        
        self.db = get_connection("robo_advisors")
        self.cursor = self.db.cursor()
        self.target_table = "{}_fund_pool".format(bank)

    def load_fund_pool(self) : 
        sql = "select FUND_NM,INTT_FUND_CD ,FUND_RISK_LEVL_CD risk ,FUND_INV_TYP_CD icode from {}".format(self.target_table)
        self.cursor.execute(sql)
        return pd.DataFrame.from_dict(self.cursor.fetchall()).set_index("INTT_FUND_CD")


class poolTsSql :
    def __init__(self,bank,service) :
        self.db = get_connection("robo_advisors")
        self.cursor = self.db.cursor()
        self.target_table = "{}_time_series_{}".format(bank,service)

    def load_fund_pool_ts(self) :
        sql = "select * from {}".format(self.target_table)
        self.cursor.execute(sql)
        return pd.DataFrame.from_dict(self.cursor.fetchall()).set_index("BASC_DT")

if __name__ == "__main__" :
    pts = poolTsSql("ibk","basic")
    pts = poolTsSql("ibk","basic")
    df = pts.load_fund_pool_ts()

    drint(df)
