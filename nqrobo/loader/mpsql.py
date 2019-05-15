from nqrobo.loader.sql_base import schema,mpFundSql
from nqrobo.loader.sql_base import *

import pandas as pd

from nqrobo.loader.id_generator import ibkBasicIdGenerator,ibkRetireIdGenerator,ibkWinIdGenerator,ibkIsaIdGenerator
from nqrobo.loader.id_generator import ibkMarketIdGenerator 
import nqrobo.loader.id_generator as ig

import sqlalchemy

class ibkMpWinReportSchema(schema) :
    def __init__(self):
        columns = ["ROBO_ID","BASC_DT","FUND_CD","FUND_INV_CD","FUND_WEIT"]
        types = [str,str,str,int,float]
        super(ibkMpWinReportSchema,self).__init__(columns,types)

class ibkMpWinReportSql(mpFundSql) :
    def __init__(self) :
        super(ibkMpWinReportSql,self).__init__("ibk","win",ibkMpWinReportSchema())
        self.target_table = "ibk_mp_report_win"

class ibkMpRetireReportSchema(schema) :
    def __init__(self) :
        columns = ["BASC_DT","ROBO_ID","MANAGE_CD","FUND_CD","ASSOC_FUND_CD","FUND_NM","FUND_WEIT"]
        types = [str,str,str,str,str,str,float]

        super(ibkMpRetireReportSchema,self).__init__(columns,types)

class ibkMpRetireReportSql(mpFundSql) :
    def __init__(self) :
        super(ibkMpRetireReportSql,self).__init__("ibk","retire",ibkMpRetireReportSchema())
        self.target_table = "ibk_mp_report_retire"

class ibkMpBasicReportSchema(schema):
    def __init__(self) :
        columns = ["BASC_DT","ROBO_ID","FUND_CD","ASSOC_FUND_CD","FUND_NM","FUN_INV_TYP_CD","FUND_WEIT"]
        types = [str,str,str,str,str,int,float]
        super(ibkMpBasicReportSchema,self).__init__(columns,types)

class ibkMpBasicReportSql(mpFundSql) :
    def __init__(self) :
        super(ibkMpBasicReportSql,self).__init__("ibk","basic",ibkMpBasicReportSchema())
        self.target_table = "ibk_mp_report_basic"

class mpFundSchemaBase(schema) :
    def __init__(self,columns,dtypes) :
        super(mpFundSchemaBase,self).__init__(columns,dtypes)

class mpFundFileSchema(mpFundSchemaBase) :
    def __init__(self) :
        pass

class mpFundFileExSchema(mpFundFileSchema) :
    def __init__(self) :
        pass

class mpFundTsSqlSchema(mpFundSchemaBase) :
    def __init__(self) :
        columns = ["BASC_DT","INTT_FUND_CD","FUND_WEIT","SLOT_NUM","ROBO_ID","MODEL_NUM"]
        dtype = [ str,str,float,int,str,int ]
        super(mpFundTsSqlSchema,self).__init__(columns,dtype)

class mpFundSqlSchema(mpFundSchemaBase) :
    def __init__(self) :
        columns = ["BASC_DT","MODEL_NUM","ROBO_ID","INTT_FUND_CD","WEIGHT","SLOT_NUM"]
        dtype = [ str,int,str,str,float,int ]
        super(mpFundSqlSchema,self).__init__(columns,dtype)

class mpFundSqlExSchema(mpFundSqlSchema) :
    def __init__(self) :
        super(mpFundSqlExSchema,self).__init__()

class ibkMpFundBasicSql(mpFundSql) :
    def __init__(self,model_num=1) :
        super(ibkMpFundBasicSql,self).__init__("ibk","basic",mpFundSqlSchema())

        self.model_num = str(model_num)
        self.id_generator = ibkBasicIdGenerator()
        self.robo_ids = self.id_generator.robo_ids
        self.target_table = "ibk_mp_basic"

        self.port_num = None
        self.csv_schema = mpFundFileSchema()

class ibkMpFundWinSql(mpFundSql) :
    def __init__(self,model_num=1) :
        super(ibkMpFundWinSql,self).__init__("ibk","win",mpFundSqlSchema())

        self.model_num = str(model_num)
        self.id_generator = ibkWinIdGenerator()
        self.robo_ids = self.id_generator.robo_ids
        self.target_table = "ibk_mp_win"

        self.port_num = None
        self.csv_schema = mpFundFileSchema()

class ibkMpFundIsaSql(mpFundSql) :
    def __init__(self,model_num=1) :
        super(ibkMpFundIsaSql,self).__init__("ibk","isa",mpFundSqlExSchema())
        self.schema.add_exception_tag("5709","5705")

        self.model_num = str(model_num)
        self.id_generator = ibkIsaIdGenerator()
        self.robo_ids = self.id_generator.robo_ids
        self.target_table = "ibk_mp_isa"

        self.port_num = None
        self.csv_schema = mpFundSqlExSchema()
        self.csv_schema.add_exception_tag("5709","5705")

class ibkMpFundRetireInternetSql(mpFundSql) :
    def __init__(self,model_num=1) :
        super(ibkMpFundRetireInternetSql,self).__init__("ibk","retire_internet",mpFundSqlSchema())

        self.model_num = str(model_num)
        self.id_generator = ibkRetireIdGenerator()
        self.robo_ids = self.id_generator.robo_ids
        self.target_table = "ibk_mp_retire_internet"

        self.port_num = None
        self.csv_schema = mpFundFileSchema()

    def get_mp_model_num(self) :
        # retire_internet , retire 가 model_num 공유 하기때문에
        # internet만 따로 override 함
        sql = "select MODEL_NUM FROM model_log where bank=\'{}\' and service_type=\'{}\'".format("ibk","retire")
        self.cursor.execute(sql)
        return self.cursor.fetchone()["MODEL_NUM"]

class ibkMpFundRetireSql(mpFundSql) :
    def __init__(self,model_num=1) :
        super(ibkMpFundRetireSql,self).__init__("ibk","retire",mpFundSqlSchema())

        self.model_num = str(model_num)
        self.id_generator = ibkRetireIdGenerator()
        self.robo_ids = self.id_generator.robo_ids
        self.target_table = "ibk_mp_retire"

        self.port_num = None
        self.csv_schema = mpFundFileSchema()

class ibkMpFundMarketSql(mpFundSql) :
    def __init__(self,model_num=1) :
        super(ibkMpFundMarketSql,self).__init__("ibk","market",mpFundSqlSchema())

        self.model_num = str(model_num)
        self.id_generator = ibkMarketIdGenerator()
        self.robo_ids = self.id_generator.robo_ids
        self.target_table = "ibk_mp_market"

        self.port_num = None
        self.csv_schema = mpFundFileSchema()

        self.port_num = 18
    
    def init_id(self,robo_id) :
        self.port_num = 18
        self.schema.init_robo_id_from_int(18)
        self.csv_schema.init_robo_id_from_int(18)

        self.ps = self.get_port_selector(18)
        self.ws = self.get_weight_selector(18)

class ibkMpFundBenchSql(mpFundSql) :
    def __init__(self,model_num=1) :
        super(ibkMpFundBenchSql,self).__init__("ibk","benchmark",mpFundSqlSchema())

        self.model_num = str(model_num)
        self.id_generator = ig.ibkBenchIdGenerator()
        self.robo_ids = self.id_generator.robo_ids
        self.target_table = "ibk_mp_benchmark"

        self.port_num = None
        self.csv_schema = mpFundSqlExSchema()
        self.init_exception_tag()

    def init_exception_tag(self) :
        win_robo_ids = self.id_generator.win_robo_ids 
        basicp_robo_ids = self.id_generator.basicp_robo_ids

        ex_tag = list(map(lambda x : [ x , x[:3] + "6" ]  ,win_robo_ids+basicp_robo_ids)) 

        list(map(lambda tag : self.csv_schema.add_exception_tag(*tag), ex_tag))


class bnkMpFundSqlBase(mpFundSql) :
    def __init__(self,b,s,sch) :
        super(bnkMpFundSqlBase,self).__init__(b,s,sch)

class bnkbMpFundBasicSql(bnkMpFundSqlBase) :
    def __init__(self,model_num=1) :
        super(bnkbMpFundBasicSql,self).__init__("bnkb","basic",mpFundSqlSchema())

        self.model_num = str(model_num)
        self.id_generator = ig.bnkbBasicIdGenerator()
        self.robo_ids = self.id_generator.robo_ids
        self.target_table = "bnkb_mp_basic"

        self.port_num = None
        self.csv_schema = mpFundFileSchema()

class bnkbMpFundBenchSql(bnkMpFundSqlBase) :
    def __init__(self,model_num=1) :
        super(bnkbMpFundBenchSql,self).__init__("bnkb","benchmark",mpFundSqlSchema())

        self.model_num = str(model_num)
        self.id_generator = ig.bnkbBenchIdGenerator()
        self.robo_ids = self.id_generator.robo_ids
        self.target_table = "bnkb_mp_benchmark"

        self.port_num = None
        self.csv_schema = mpFundSqlExSchema()
        self.init_exception_tag()

    def init_exception_tag(self) :
        bench_robo_ids = self.id_generator.robo_ids

        ex_tag = list(map(lambda x : [ x , x[:3] + "6" ]  , bench_robo_ids)) 

        list(map(lambda tag : self.csv_schema.add_exception_tag(*tag), ex_tag))

class bnkbMpFundProBasicSql(bnkMpFundSqlBase) :
    def __init__(self,model_num=1) :
        super(bnkbMpFundProBasicSql,self).__init__("bnkb","pro_basic",mpFundSqlSchema())

        self.model_num = str(model_num)
        self.id_generator = ig.bnkbProBasicIdGenerator()
        self.robo_ids = self.id_generator.robo_ids
        self.target_table = "bnkb_mp_pro_basic"

        self.port_num = None
        self.csv_schema = mpFundFileSchema()

class bnkbMpFundRetireSql(bnkMpFundSqlBase) :
    def __init__(self,model_num=1) :
        super(bnkbMpFundRetireSql,self).__init__("bnkb","retire",mpFundSqlSchema())

        self.model_num = str(model_num)
        self.id_generator = ig.bnkbRetireIdGenerator()
        self.robo_ids = self.id_generator.robo_ids
        self.target_table = "bnkb_mp_retire"

        self.port_num = None
        self.csv_schema = mpFundFileSchema()

################################

class bnkkMpFundBasicSql(bnkMpFundSqlBase) :
    def __init__(self,model_num=1) :
        super(bnkkMpFundBasicSql,self).__init__("bnkk","basic",mpFundSqlSchema())

        self.model_num = str(model_num)
        self.id_generator = ig.bnkkBasicIdGenerator()
        self.robo_ids = self.id_generator.robo_ids
        self.target_table = "bnkk_mp_basic"

        self.port_num = None
        self.csv_schema = mpFundFileSchema()

class bnkkMpFundBenchSql(bnkMpFundSqlBase) :
    def __init__(self,model_num=1) :
        super(bnkkMpFundBenchSql,self).__init__("bnkk","benchmark",mpFundSqlSchema())

        self.model_num = str(model_num)
        self.id_generator = ig.bnkkBenchIdGenerator()
        self.robo_ids = self.id_generator.robo_ids
        self.target_table = "bnkk_mp_benchmark"

        self.port_num = None
        self.csv_schema = mpFundSqlExSchema()
        self.init_exception_tag()

    def init_exception_tag(self) :
        bench_robo_ids = self.id_generator.robo_ids

        ex_tag = list(map(lambda x : [ x , x[:3] + "6" ]  , bench_robo_ids)) 

        list(map(lambda tag : self.csv_schema.add_exception_tag(*tag), ex_tag))

class bnkkMpFundProRetireSql(bnkMpFundSqlBase) :
    def __init__(self,model_num=1) :
        super(bnkkMpFundProRetireSql,self).__init__("bnkk","pro_retire",mpFundSqlSchema())

        self.model_num = str(model_num)
        self.id_generator = ig.bnkkProRetireIdGenerator()
        self.robo_ids = self.id_generator.robo_ids
        self.target_table = "bnkk_mp_pro_retire"

        self.port_num = None
        self.csv_schema = mpFundFileSchema()

class bnkkMpFundProBasicSql(bnkMpFundSqlBase) :
    def __init__(self,model_num=1) :
        super(bnkkMpFundProBasicSql,self).__init__("bnkk","pro_basic",mpFundSqlSchema())

        self.model_num = str(model_num)
        self.id_generator = ig.bnkkProBasicIdGenerator()
        self.robo_ids = self.id_generator.robo_ids
        self.target_table = "bnkk_mp_pro_basic"

        self.port_num = None
        self.csv_schema = mpFundFileSchema()

class bnkkMpFundRetireSql(bnkMpFundSqlBase) :
    def __init__(self,model_num=1) :
        super(bnkkMpFundRetireSql,self).__init__("bnkk","retire",mpFundSqlSchema())

        self.model_num = str(model_num)
        self.id_generator = ig.bnkkRetireIdGenerator()
        self.robo_ids = self.id_generator.robo_ids
        self.target_table = "bnkk_mp_retire"

        self.port_num = None
        self.csv_schema = mpFundFileSchema()


BNKB_CLASSES = [ bnkbMpFundBasicSql,bnkbMpFundRetireSql,bnkbMpFundBenchSql,bnkbMpFundProBasicSql]
BNKK_CLASSES = [ bnkkMpFundBasicSql,bnkkMpFundRetireSql,bnkkMpFundBenchSql,bnkkMpFundProBasicSql,bnkkMpFundProRetireSql]
IBK_CLASSES = [ibkMpFundBasicSql ,ibkMpFundRetireSql, ibkMpFundRetireInternetSql, ibkMpFundMarketSql,ibkMpFundBenchSql,ibkMpFundWinSql,ibkMpFundIsaSql]