from nqrobo.loader.sql_base import get_connection,sqlBase,mpFundSql
from nqrobo.loader.zeroin_sql import zeroinSql
from nqrobo.loader.mpsql import mpFundSqlSchema,schema,mpFundSchemaBase
from nqrobo.loader.mpsql import mpFundFileSchema,mpFundTsSqlSchema

import nqrobo.loader.mpsql as mp

from nqrobo.loader.id_generator import ibkBasicIdGenerator,ibkRetireIdGenerator,ibkIsaIdGenerator,ibkWinIdGenerator
from qrobo.utils.path import env

import nqrobo.loader.id_generator as ig

import os
import pandas as pd

import sys

def srtdf(series) :
    '''
        Series 를 DF로
    '''
    df = pd.DataFrame(index=[series.name],columns=series.index.values)
    df.iloc[:,:] = series.values
    return df

'''
TS schema 가 따로 구현되어 있는 이유는
로보 아이디 마다 port 개수가 다르기 때문
init_robo_id 호출로 state 변경 후 사용해야함
'''

class mpTsSchema(mpFundSchemaBase) :
    def __init__(self) :
        pass

    def init_robo_id(self,robo_id) :
        super(mpTsSchema,self).init_robo_id(robo_id)
        self.columns += ["NET_ASSET"]
        self.types += [float]

    def add_exception_tag(self,robo_id,rp_robo_id) :
        self.ex_dict = {} if not hasattr(self,"ex_dict") else self.ex_dict
        self.ex_dict[robo_id] = rp_robo_id

class mpTsSqlSchema(schema)  :
    def __init__(self) :
        types = [str,str,float,int,str,int]
        columns = ["BASC_DT","INTT_FUND_CD","FUND_WEIT","SLOT_NUM","ROBO_ID","MODEL_NUM"] 
        super(mpTsSqlSchema,self).__init__(columns,types)

    def init_robo_id(self,r) :
        pass

class mpTsSqlBase(mpFundSql) :
    tclass = ["id_generator","robo_ids"]
    t_back_up_dirs = {
        "ibk" : [ "basic","win","benchmark","isa","retire","retire_internet","market"],
        "bnkb" : [ "basic","pro_basic","retire","benchmark"],
        "bnkk" : [ "basic","pro_basic","retire","pro_retire","benchmark"]
    }

    def __init__(self,bank,service) :

        schema = mpFundTsSqlSchema()
        super(mpTsSqlBase,self).__init__(bank,service,schema)
        self.csv_schema = mpTsSchema()
        self.schema = mpTsSqlSchema()
        self.robo_id = None
        self.target_table = "{}_mp_time_series_{}".format(bank,service)
        self.dir = env.qrobo_HOME + "/run_future/models/{}/{}/".format(bank,service)
        self.rdir = env.qrobo_HOME + "/run_future/"

        # 순환 참조 방지 하기 위해 런타임에 임포트
        # classBuilder 에서도 tssql 모듈을 임포트 하기 때문
        from qrobo.server.class_builder import classBuilder
        classBuilder.bind(self,bank,service,*self.tclass)

    @staticmethod
    def make_back_up_dirs(date) :
        '''
            백업하기전 한번만 호출하면됨
        '''
        if type(date) == pd.Timestamp :
            date = date.strftime("%Y%m%d")

        dir_map = mpTsSqlBase.t_back_up_dirs
        
        try :
            os.chdir(env.qrobo_HOME + "/run_future/")
            os.mkdir(date)
            os.chdir(date)

            for bank in dir_map : 
                os.mkdir(bank)
                os.chdir(bank)                

                for service in dir_map[bank] :
                    os.mkdir(service)
                    os.chdir(service) 

                    for model_num in range(1,10) :
                        os.mkdir("model_{}".format(model_num))

                    os.chdir("../")

                os.chdir("../")

        except FileExistsError :
            pass

    def get_back_up_dir(self,robo_id,date) :
        return self.rdir + "{}/{}/{}/model_{}/{}".format(date,self.bank,self.service_type,self.model_num,robo_id) + ".csv"

    def get_time_series_only_net(self,model_num,robo_id) :
        sql = "SELECT BASC_DT, SUM(FUND_WEIT) NET_ASSET\
	           FROM {}\
               WHERE ROBO_ID = {} and MODEL_NUM = {}\
               GROUP BY BASC_DT\
	           ORDER BY BASC_DT;".format(self.target_table,robo_id,model_num)

        print(sql)
        self.cursor.execute(sql)

        v = self.cursor.fetchall()
        v = pd.DataFrame.from_dict(v).set_index("BASC_DT")
        v.index = pd.to_datetime(v.index)

        return v 

    def set_model_num(self,mn) :
        self.model_num = mn

    def mp_csv_to_mp_ts_sql(self) :
        ts = self.load_csv_file_from_robo_id(self.robo_id)        

        for pi in range(self.port_num) :
            bdf = self.generate_ts_row_dict(ts,pi)
            self.insert_bulk_df(bdf)

        return self.db.comit()

    def insert_ts_file_to_db(self,model_num,robo_id) :
        self.set_model_num(model_num)
        self.init_id(robo_id)

        df = self.load_csv_file_from_robo_id(self.robo_id)
        df = df.dropna()

        self.insert_ts_df_to_db(df)
        return self.db.commit()

    def duplicate_yester_day_data_all(self,today) :
        for ri in self.robo_ids :
            self.duplicate_yester_day_data(today,self.model_num,ri)

        return self.db.commit()

    def duplicate_yester_day_data(self,today,model_num,robo_id) :
        # 주말 데이터 채우는 로직
        # commit 은 caller가 하셈
        today = pd.Timestamp(today)
        yester_day = today - pd.Timedelta("1 Days")

        ts = self.get_time_series(model_num,robo_id)

        res = ts.loc[yester_day]
        res.name = today.strftime("%Y-%m-%d")
        df = srtdf(res)

        return self.insert_ts_df_to_db(df)

    def insert_ts_df_to_db(self,df) :
        '''
            DB Commit is up to Caller.
        '''

        for sn in range(self.port_num) :
            dict_df = self.generate_ts_row_dict(df,sn)

            v = list(map(lambda x : tuple(x), dict_df.values.tolist()))
            self.insert_values_s(self.target_table,v) 

    def generate_ts_row_dict(self,df,sn) :
        ps = "portfolio_{}".format(sn+1)
        ws = "portfolio_weight_{}".format(sn+1)

        fund_codes = df.loc[:,ps].values
        fund_weights = df.loc[:,ws].values
        dates = df.index.values

        obj = lambda fc,fw,d : { "BASC_DT" : pd.Timestamp(d).strftime("%Y%m%d") , "INTT_FUND_CD" : fc, "FUND_WEIT" : fw, "SLOT_NUM" :sn, "MODEL_NUM":self.model_num,"ROBO_ID":self.robo_id}
        values = list(map(obj,fund_codes,fund_weights,dates))

        df = pd.DataFrame.from_dict(values)
        return df.loc[:,self.schema.columns]

    def get_time_series(self,model_num,robo_id) :
        self.set_model_num(model_num)
        self.init_id(robo_id)
        sql = "SELECT * FROM {} where ROBO_ID ={} AND MODEL_NUM = {} order by BASC_DT".format(self.target_table,robo_id,model_num)
        print(sql)
        self.cursor.execute(sql)

        dictv = self.cursor.fetchall()
        pdf = pd.DataFrame.from_dict(dictv).set_index("BASC_DT")

        basc_dt = self.check_ts_index_valid(pdf) 

        return self.convert_dict_to_ts_df(pdf,basc_dt)

    def check_ts_index_valid(self,sql_df) :
        for sn in range(self.port_num) :
            s = sql_df[ sql_df["SLOT_NUM"] == sn ]
            s = s.dropna()

            index = s.index

            if sn == 0  :
                _index = index

            assert(_index.size == index.size)

        return index

    def convert_dict_to_ts_df(self,df,basc_dt) :

        ndf = pd.DataFrame(index=basc_dt,columns=self.csv_schema.columns) 

        for sn in range(self.port_num) :
            sp = "portfolio_{}".format(sn+1)
            sw = "portfolio_weight_{}".format(sn+1)

            v = df[ df["SLOT_NUM"] == sn ]

            assert(v.index.size == ndf.index.size )

            ndf[sp] = v["INTT_FUND_CD"]
            ndf[sw] = v["FUND_WEIT"]

        psa = self.get_weight_selector(self.port_num)
        ndf["NET_ASSET"] = ndf.apply(lambda x : x[psa].sum() ,axis=1)

        return ndf

    def load_csv_file_from_robo_id(self,robo_id) :
        self.csv_schema.init_robo_id(robo_id)

        file_path = env.qrobo_HOME + "\\run_future\\models\\{}\\{}\\model_{}\\".format(self.bank,self.service_type,self.model_num)
        files = os.listdir(file_path)
        target_files = list(filter(lambda x : robo_id in x ,files))

        #assert( len(target_files) <= 2 )

        target_file = list(filter(lambda x : "net_asset" in x, target_files))
        assert( len(target_file) == 1 )

        target_file = target_file[0]

        print(target_file)
        target_file = file_path + target_file

        v = pd.read_csv(target_file,index_col=0,encoding="euc-kr")
        v.index = pd.to_datetime(v.index)

        return v

    def get_expected_row_count_d(self) :
        def obj(r) :
            if hasattr(self.csv_schema,"ex_dict") and r in self.csv_schema.ex_dict :
                r = self.csv_schema.ex_dict[r]

            return int(r[-1])

        return sum(list(map(lambda x : obj(x) , self.robo_ids)))

class bnkbMpTsSqlBasic(mpTsSqlBase) :
    def __init__(self) :
        super(bnkbMpTsSqlBasic,self).__init__("bnkb","basic")

class bnkbMpTsSqlRetire(mpTsSqlBase) :
    def __init__(self) :
        super(bnkbMpTsSqlRetire,self).__init__("bnkb","retire")

class bnkbMpTsSqlProBasic(mpTsSqlBase) :
    def __init__(self) :
        super(bnkbMpTsSqlProBasic,self).__init__("bnkb","pro_basic")

class bnkbMpTsSqlBench(mpTsSqlBase) :
    def __init__(self) :
        super(bnkbMpTsSqlBench,self).__init__("bnkb","benchmark")
        self.init_exception_tag()

    def init_exception_tag(self) :
        bench_robo_ids = self.id_generator.robo_ids
        ex_tag = list(map(lambda x : [ x , x[:3] + "6" ]  , bench_robo_ids)) 
        list(map(lambda tag : self.csv_schema.add_exception_tag(*tag), ex_tag))

class bnkkMpTsSqlBasic(mpTsSqlBase) :
    def __init__(self) :
        super(bnkkMpTsSqlBasic,self).__init__("bnkk","basic")

class bnkkMpTsSqlRetire(mpTsSqlBase) :
    def __init__(self) :
        super(bnkkMpTsSqlRetire,self).__init__("bnkk","retire")

class bnkkMpTsSqlProRetire(mpTsSqlBase) :
    def __init__(self) :
        super(bnkkMpTsSqlProRetire,self).__init__("bnkk","pro_retire")

class bnkkMpTsSqlProBasic(mpTsSqlBase) :
    def __init__(self) :
        super(bnkkMpTsSqlProBasic,self).__init__("bnkk","pro_basic")

class bnkkMpTsSqlBench(mpTsSqlBase) :
    def __init__(self) :
        super(bnkkMpTsSqlBench,self).__init__("bnkk","benchmark")
        self.init_exception_tag()

    def init_exception_tag(self) :
        bench_robo_ids = self.id_generator.robo_ids
        ex_tag = list(map(lambda x : [ x , x[:3] + "6" ]  , bench_robo_ids)) 
        list(map(lambda tag : self.csv_schema.add_exception_tag(*tag), ex_tag))

class ibkMpTsSqlBasic(mpTsSqlBase) :
    def __init__(self) :
        super(ibkMpTsSqlBasic,self).__init__("ibk","basic")

class ibkMpTsSqlRetireInternet(mpTsSqlBase) :
    def __init__(self) :
        super(ibkMpTsSqlRetireInternet,self).__init__("ibk","retire_internet")

class ibkMpTsSqlRetire(mpTsSqlBase) :
    def __init__(self) :
        super(ibkMpTsSqlRetire,self).__init__("ibk","retire")

class ibkMpTsSqlIsa(mpTsSqlBase) :
    def __init__(self) :
        super(ibkMpTsSqlIsa,self).__init__("ibk","isa")
        self.csv_schema.add_exception_tag("5709","5705")

class ibkMpTsSqlWin(mpTsSqlBase) :
    def __init__(self) :
        super(ibkMpTsSqlWin,self).__init__("ibk","win")

class ibkMpTsSqlMarket(mpTsSqlBase) :
    def __init__(self) :
        super(ibkMpTsSqlMarket,self).__init__("ibk","market")
        self.csv_schema = mpFundFileSchema()

    def init_id(self,robo_id) :
        self.port_num = 18
        self.csv_schema.robo_id = robo_id
        self.csv_schema.init_robo_id_from_int(18)
        self.robo_id = robo_id

    def get_expected_row_count_d(self) :
        return len(self.robo_ids) * 18

class ibkMpTsSqlBench(mpTsSqlBase) :
    def __init__(self) :
        super(ibkMpTsSqlBench,self).__init__("ibk","benchmark")
        self.init_exception_tag_win()

    def init_exception_tag_win(self) :
        win_robo_ids = self.id_generator.win_robo_ids
        basicp_robo_ids = self.id_generator.basicp_robo_ids

        ex_tag = list(map(lambda x : [ x , x[:3] + "6" ]  ,win_robo_ids+basicp_robo_ids)) 

        list(map(lambda tag : self.csv_schema.add_exception_tag(*tag), ex_tag))

class tsSqlBase(sqlBase) :
    def __init__(self,bank) :
        super(tsSqlBase,self).__init__()
        self.db = get_connection("robo_advisors")
        self.cursor = self.db.cursor()
        self.bank = bank
        self.target_tables = [ ] 

def getc(pref,surf) :
    return eval(pref+surf)

class ibkMpTsSql(tsSqlBase) :
    def __init__(self) :
        super(ibkMpTsSql,self).__init__("ibk")
        self.target_tables = ["basic","retire","retire_internet","isa","win","benchmark","market"]
        self.target_tables = list(map(lambda x : "ibk_mp_time_series_{}".format(x),self.target_tables))

        ### SELF.SURFS 는 db script 사용시 루프 돌리면서 콜백 함수 돌리려고 편의상 쓰는 변수
        ### run_future/scripts/db_script.py
        self.backtest_targets = ["Basic","Retire","RetireInternet","Isa","Win","Bench","Market"]
        self.surfs = ["Basic","Retire","RetireInternet","Isa","Win","Bench","Market"]
        self.expected_row_counts = list(map(lambda x : x.get_expected_row_count_d() , list(map(lambda x : getc("ibkMpTsSql",x)() , self.surfs ))))
        self.clss = [ ibkMpTsSqlBasic,ibkMpTsSqlRetire,ibkMpTsSqlRetireInternet,ibkMpTsSqlIsa,ibkMpTsSqlWin,ibkMpTsSqlBench,ibkMpTsSqlMarket]
        self.mclss = list(map(lambda x : getattr(mp, "ibkMpFund{}Sql".format(x)) , self.surfs))
        assert( len(self.clss) == len(self.mclss) )

class bnkkMpTsSql(tsSqlBase) :
    def __init__(self) :
        super(bnkkMpTsSql,self).__init__("bnkk")
        self.target_tables = ["basic","retire","pro_basic","pro_retire","benchmark"]
        self.target_tables = list(map(lambda x : "bnkk_mp_time_series_{}".format(x),self.target_tables))
        self.surfs = ["Basic","Retire","ProBasic","ProRetire","Bench"] 
        self.backtest_targets = ["Basic","Retire","ProBasic","ProRetire","Bench"] 
        self.expected_row_counts = list(map(lambda x : x.get_expected_row_count_d() , list(map(lambda x : getc("bnkkMpTsSql",x)() , self.surfs ))))
        self.clss = [ bnkkMpTsSqlBasic,bnkkMpTsSqlRetire,bnkkMpTsSqlProBasic,bnkkMpTsSqlProRetire,bnkkMpTsSqlBench]
        self.mclss = [ mp.bnkkMpFundBasicSql,mp.bnkkMpFundRetireSql,mp.bnkkMpFundProBasicSql,mp.bnkkMpFundProRetireSql,mp.bnkkMpFundBenchSql]

        assert( len(self.clss) == len(self.mclss) )

class bnkbMpTsSql(tsSqlBase) :
    def __init__(self) :
        super(bnkbMpTsSql,self).__init__("bnkb")
        self.target_tables = ["basic","retire","pro_basic","benchmark"]
        self.target_tables = list(map(lambda x : "bnkb_mp_time_series_{}".format(x),self.target_tables))
        self.surfs = ["Basic","Retire","ProBasic","Bench"] 
        self.backtest_targets = ["Basic","Retire","ProBasic","Bench"] 
        self.expected_row_counts = list(map(lambda x : x.get_expected_row_count_d() , list(map(lambda x : getc("bnkbMpTsSql",x)() , self.surfs ))))
        self.clss = [ bnkbMpTsSqlBasic,bnkbMpTsSqlRetire,bnkbMpTsSqlProBasic,bnkbMpTsSqlBench]
        self.mclss = [ mp.bnkbMpFundBasicSql,mp.bnkbMpFundRetireSql,mp.bnkbMpFundProBasicSql,mp.bnkbMpFundBenchSql]

        assert( len(self.clss) == len(self.mclss) )

class ibkTsSql(tsSqlBase) :
    def __init__(self) :
        super(ibkTsSql,self).__init__("ibk")
        self.target_tables = [ "ibk_time_series_basic","ibk_time_series_retire","ibk_time_series_isa","ibk_time_series_win","thomson_time_series" ]

class bnkbTsSql(tsSqlBase) :
    def __init__(self) :
        super(bnkbTsSql,self).__init__("bnkb")
        self.target_tables = ["bnkb_time_series_basic","bnkb_time_series_retire","thomson_time_series"]

class bnkkTsSql(tsSqlBase) :
    def __init__(self) :
        super(bnkkTsSql,self).__init__("bnkk")
        self.target_tables = ["bnkk_time_series_basic","bnkk_time_series_retire","thomson_time_series"]

class zeroinTsSql(tsSqlBase) :
    def __init__(self) :
        zsql = zeroinSql()
        self.db = zsql.db
        self.cursor = self.db.cursor()
        self.target_tables = zsql.zeroin_tables
        self.loop_count = -1

def task() :
    import nqrobo.loader.mpsql as mps

    i = ibkMpTsSqlBasic()
    i.set_model_num(1)
    mp = mps.ibkMpFundBasicSql(1)

    for r in i.robo_ids :
        df = i.load_csv_file_from_robo_id(r)
        m = mp.get_target_mp_from_file(r)

        ps = mp.get_port_selector(mp.port_num)
        df.loc[:,ps] = None

        for idx,v in m.iterrows() :
            df.loc[idx, ps] = m.loc[idx,ps]

        df = df.fillna(method="ffill")

        n =  mp.get_target_file_name(r).replace(".csv","_net_asset.csv")
        print(n)
        df.to_csv(n)


BNKB_CLASSES = [ bnkbMpTsSqlBasic,bnkbMpTsSqlRetire,bnkbMpTsSqlBench , bnkbMpTsSqlProBasic]
BNKK_CLASSES = [ bnkkMpTsSqlBasic,bnkkMpTsSqlRetire,bnkkMpTsSqlBench , bnkkMpTsSqlProBasic,bnkkMpTsSqlProRetire]
IBK_CLASSES = [ibkMpTsSqlBasic,ibkMpTsSqlBench,ibkMpTsSqlIsa,ibkMpTsSqlRetire,ibkMpTsSqlRetireInternet,ibkMpTsSqlWin]

if __name__ == "__main__" :

    #i.add_uniq("ibk_mp_time_series_benchmark","BASC_DT,SLOT_NUM,ROBO_ID,MODEL_NUM")
    #i.insert_ts_file_to_db(1,"2415")
    i = ibkMpTsSqlBasic()
    i.duplicate_yester_day_data("20190406",1,"1103")

    sys.exit(0)
    
    for r in i.robo_ids :
        i.insert_ts_file_to_db(1,r)

    sys.exit(0)
    for bc in BNKK_CLASSES[3:] :
        i = bc() 
         
        for r in i.robo_ids:
            i.insert_ts_file_to_db(1,r)

