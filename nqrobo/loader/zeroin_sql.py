import pymysql
from nqrobo.loader.sql_base import *
import pandas as pd

class zeroinSql(sqlBase) :
    def __init__(self) :
        self.db = get_connection("zeroin")
        self.cursor = self.db.cursor()
        self.zeroin_tables = list(map(lambda x : "zeroin_time_series_{}".format(x),range(1,20))) + ["zeroin_time_series_exception"]
        self.target_tables = self.zeroin_tables

    def check_zeroin_table_target_date(self,date) :

        def o(t) :
            r = self.get_data_with_target_date(t,date)
            assert(len(r) == 1)
            return len(r)

        return self.execute_all_zeroin_tables(o)

    def execute_all_zeroin_tables(self,_lambda,commit=True,**kwargs) :
        for ztable in self.zeroin_tables :
            print(ztable)
            code = _lambda(ztable,**kwargs)
            print("{} row Affected in {}".format(code,ztable)) 
        
        if commit :
            self.db.commit()

    def dup_and_insert_row_data_all_ztable(self,today,day_duplicated) :
        kw = {
            "today" : today,
            "day_duplicated" : day_duplicated
        }

        self.execute_all_zeroin_tables(self.duplicate_row_data,**kw)

    def duplicate_row_data(self,ztable,today,day_duplicated) :
        sql = "SELECT * FROM {} WHERE BASC_DT = {}".format(ztable,day_duplicated)

        r = self.cursor.execute(sql)
        columns = self.cursor.description

        assert(r == 1)

        v = list(self.cursor)[0]
        v["BASC_DT"] = pd.Timestamp(today)
        value_str = "("+ v["BASC_DT"].strftime("%Y%m%d") + ","

        for col in columns[1:] :
            col = col[0]
            value_str += add_colon_if_value_str(v[col])
            value_str += ","

        value_str = value_str[:-1]
        value_str += ")"
        sql = "INSERT INTO {} VALUES {}".format(ztable,value_str)

        try : 
            rc = self.cursor.execute(sql) 
            return rc

        except pymysql.err.IntegrityError :
            sql = "REPLACE INTO {} VALUES {}".format(ztable,value_str)
            return self.cursor.execute(sql)

if __name__ == "__main__" :
    zs = zeroinSql()
    r =  zs.get_data_with_target_date("zeroin_time_series","20190320")
    zs.check_zeroin_table_target_date("20190320")

