import pymysql
import pandas as pd
from functools import reduce
import numpy as np
import sys
from qrobo.utils.path import env
import qrobo.utils.path as qpath
import sqlalchemy

def generate_mp_column_name_from_id(robo_id) :
    port_num = get_port_num_from_id(robo_id)
    return generate_mp_column_name(port_num)

def get_port_num_from_id(robo_id) :
    return int(robo_id[-1])

def generate_mp_column_name(num_fund) :
    wlist = list(map(lambda x : "portfolio_weight_{}".format(x), range(1,num_fund+1)))
    plist = list(map(lambda x : "portfolio_{}".format(x), range(1,num_fund+1)))
    return wlist + plist

def generate_mp_type(num_fund) :
    wtype = list(map(lambda x : float,range(num_fund)))
    ptype = list(map(lambda x : str ,range(num_fund)))
    return wtype + ptype

#TODO Parameter 하나의 인스턴스로만 사용하게

pymysql.install_as_MySQLdb()

def get_connection(db_name) :
    return pymysql.connect(host=env.qrobo_DBHOST, 
                                  port=int(env.qrobo_DBPORT), user=env.qrobo_DBUSER, passwd=env.qrobo_DBPWD, db=db_name, charset='utf8',
                                  cursorclass=pymysql.cursors.DictCursor)

def get_pd_connection(db_name) :
    query = 'mysql://{}:{}@{}:{}/{}?charset=utf8'
    query = query.format(env.qrobo_DBUSER, 
                            env.qrobo_DBPWD, 
                            env.qrobo_DBHOST, 
                            env.qrobo_DBPORT, 
                            db_name)
    return sqlalchemy.create_engine(query, pool_recycle=1)

def add_colon_if_value_str(value) :
    if type(value) == str :
        return "\"" + value + "\""
    else :
        return str(value)

class schema :
    def __init__(self,columns,types) :
        self.columns = columns
        self.types = types

        self.dtype = reduce(lambda x , y : x + [( y ,self.types[y] )] , range(len(self.types)),[])
        self.dtype = dict(self.dtype)        

    def add_exception_tag(self,robo_id,rep_id) :
        self.ex_dict = {} if not hasattr(self,"ex_dict") else self.ex_dict
        self.ex_dict[robo_id] = rep_id

    def init_robo_id(self,robo_id) :
        if hasattr(self,"ex_dict") and robo_id in self.ex_dict :
            self._init_robo_id(self.ex_dict[robo_id])
        else :
            self._init_robo_id(robo_id)

    def _init_robo_id(self,robo_id) :
        port_num = int(robo_id[-1])
        self.init_robo_id_from_int(port_num)
    
    def init_robo_id_from_int(self,plen) :
        c = generate_mp_column_name(plen)
        p = [str]+generate_mp_type(plen)

        schema.__init__(self,c,p)

class sqlBase :
    def __init__(self) :
        self.cursor = None
        self.db = None
        self.bank = None
        self.service_type = None
        self.schema = None
        self.target_table = []
        self.sql_history = []
        self.loop_count = -1

    def step_in_loop(self) :
        self.loop_count += 1 
        return self.loop_count

    def loop_done(self) :
        self.loop_count = -1

    def for_each(self,lamb) :
        
        if not hasattr(self,"loop_count") :
            self.loop_count = -1

        def obj(t) :
            self.step_in_loop()
            return lamb(self,t)

        res = list(map(lambda x : obj(x),self.target_tables))

        self.loop_done()

        return res

    def add_uniq(self,table,cname_or_cnames,uidx_name="uidx") :
        sql = "ALTER TABLE {} ADD CONSTRAINT {} UNIQUE ({});".format(table,uidx_name,cname_or_cnames)
        print(sql) 
        return self.cursor.execute(sql)

    def add_index(self,ztable,cname_or_cnames,idxname="didx") :
        sql = "ALTER TABLE {} ADD INDEX {}({});".format(ztable,idxname,cname_or_cnames)
        print(sql)
        return self.cursor.execute(sql)

    def remove_date_is_null(self,ztable) :
        sql = "DELETE FROM {} WHERE BASC_DT IS NULL;".format(ztable)
        return self.cursor.execute(sql)

    def make_field_update_query(self,df) :
        query = ""

        for col in df.columns.values :
            query += col + "=" 
            query += add_colon_if_value_str(df[col].values[0])
            query += ","

        return query[:-1]   

    def remove_target_date(self,ztable,date) :
        sql = "DELETE FROM {} WHERE BASC_DT = {}".format(ztable,date)
        return self.cursor.execute(sql) 

    def update_target_date(self,df,ztable,date):
        cond = "BASC_DT = \"{}\"".format(date)
        fields = self.make_field_update_query(df)
        sql = "UPDATE {} SET {} WHERE {}".format(ztable,fields,cond)

        self.cursor.execute(sql)
        self.db.commit()

    def get_data_with_where(self,table,date="20010101",op=">=",limit=1000) :
        sql = "SELECT * FROM {} where BASC_DT {} {} order by BASC_DT limit {}".format(table,op,date,limit)
        self.cursor.execute(sql)
        return self.cursor.fetchall()

    def get_data_no_order(self,table) :
        sql = "SELECT * FROM {}".format(table)
        self.cursor.execute(sql)
        return self.cursor.fetchall()

    def get_data(self,table) :
        sql = "SELECT * FROM {} order by BASC_DT ".format(table)
        print(sql)
        self.cursor.execute(sql)
        return self.cursor.fetchall()

    def get_data_df(self,table) :
        r = self.get_data(table)
        df = pd.DataFrame.from_dict(r)
        df = df.fillna(method="ffill")
        return df.set_index("BASC_DT")

    def get_data_with_target_date(self,table,date,limit=1000) :
        sql = "SELECT * FROM {} where BASC_DT = \'{}\'".format(table,date)
        self.cursor.execute(sql)
        return self.cursor.fetchall()

    def try_insert_values(self,table,vstring) :
        sql = "INSERT INTO {} VALUES {};".format(table,vstring)
        print(sql)
        try : 
            return self.execute_sql(sql)
        except pymysql.err.IntegrityError  as e : 
            print(e)
            return -1

    def insert_values(self,table,vstring) :
        sql = "INSERT INTO {} VALUES {};".format(table,vstring)
        print(sql)
        return self.execute_sql(sql)

    def execute_sql(self,sql) :
        now = pd.Timestamp("now").strftime("%Y-%m-%d %H:%M:%S")
        self.sql_history.append([ now, sql ])
        return self.cursor.execute(sql)

    def commit_sql_log(self) :
        def to_query_and_exe(sql_log) :
            '''
                sql_log[0] - BASC_DT
                sql_log[1] - Query value.
            '''
            sql = "INSERT INTO mp_sql_log values (\'{}\',\'{}\',\'{}\',\'{}\')".format(sql_log[0], self.bank,self.service_type,sql_log[1])
            print(sql)
            return self.cursor.execute(sql)

        res = list(filter(lambda x : not to_query_and_exe(x),self.sql_history))
        self.sql_history = []

        return  len(res) == 0 

    def insert_values_s(self,table,datas) :
        columns_strings = reduce(lambda x , y : x + "," + y ,self.schema.columns )
        cc = len(self.schema.columns)

        vstring = reduce(lambda x ,y : str(x) + "%s," ,range(cc),"")
        vstring = vstring[:-1]

        sql = "INSERT INTO {} ({}) VALUES ("
        sql = sql.format(table,columns_strings) + vstring
        sql += ");"

        return self.cursor.executemany(sql,datas)

    def df_to_value_query_string(self,df) :
        ret = df.apply(lambda k : reduce(lambda x,y : str(x) + "," + add_colon_if_value_str(y),k),axis=1)
        return ret.apply(lambda x : "(" + x + ")")

    def read_csv(self,file_name,schema=None,index_col=0) :
        return self.read_to_df(file_name,dim=",",schema=self.csv_schema,header=0,index_col=index_col)

    def read_dat(self,file_name,schema=None,index_col=0) :
        return self.read_to_df(file_name,dim="|",schema=schema,index_col=index_col)

    def read_to_df(self,file_name,dim,schema=None,header=None,index_col=0):

        if schema is None :
            schema = self.schema

        try :
            #columns = self.schema.columns + ["NET_ASSET"]            
            #dtype = { **self.schema.dtype ,**{ len(self.schema.columns) + 1 : "float" } } 

            mp = pd.read_csv(file_name,delimiter=dim,header=header, 
                              dtype=schema.dtype,index_col=index_col)

        except UnicodeDecodeError :
            mp = pd.read_csv(file_name,delimiter=dim,header=header, 
                              dtype=schema.dtype,encoding="euc-kr",index_col=index_col)

        except Exception as e :
            print(e)
            sys.exit(1)

        mp.columns = schema.columns 
        mp.index.name = "BASC_DT"
        mp.index = pd.to_datetime(mp.index)

        return mp

class netAssetSql(sqlBase) :
    def __init__(self,bank,service_type,schema) :
        self.schema = schema
        self.db = get_connection("robo_advisors") 
        self.bank = bank
        self.service_type = service_type

class mpFundSql(sqlBase) :
    def __init__(self,bank,service_type,schema) :
        super(mpFundSql,self).__init__()
        self.model_num = None
        self.port_num = None
        
        self.schema = schema 
        self.db = get_connection("robo_advisors")

        self.bank = bank
        self.service_type = service_type
        self.cursor = self.db.cursor()
        self.target_table = None

    def get_port_selector(self,nfund) :
        return list(map(lambda x : "portfolio_{}".format(x),range(1,nfund+1)))

    def get_weight_selector(self,nfund) :
        return list(map(lambda x : "portfolio_weight_{}".format(x),range(1,nfund+1)))

    def get_mp_with_target_date(self,date) :
        return self.get_data_with_where("{}_mp_{}".format(self.bank,self.service_type),op="=",date=date)

    def get_mp_model_num(self) :
        sql = "select MODEL_NUM FROM model_log where bank=\'{}\' and service_type=\'{}\'".format(self.bank,self.service_type)
        self.cursor.execute(sql)
        return self.cursor.fetchone()["MODEL_NUM"]

    def get_robo_ids_in_date(self,date) :
        if type(date) == pd.Timestamp :
            date = date.strftime("%Y-%m-%d")

        sql = "select BASC_DT , SUM(FUND_WEIT) S, ROBO_ID from {} \
               group by MODEL_NUM,ROBO_ID,BASC_DT \
               having BASC_DT = \'{}\' and MODEL_NUM={}".format(self.target_table,date,self.model_num)
        
        print(sql)
        self.cursor.execute(sql)            
        v = self.cursor.fetchall()

        if len(v) == 0 :
            return []

        df = pd.DataFrame.from_dict(v).set_index("ROBO_ID")
        self.check_slot_valid(df)

        return df.index.values.tolist()

    def check_slot_valid(self,df) :

        # SLOT이 제대로 들어갔는지 체크
        robo_ids = sorted(df.index.values)

        rdf = df [ df["S"] > 0.99999 ]
        rdf = rdf [ rdf["S"] < 1.00001 ]

        crobo_ids = sorted(rdf.index.values)

        assert( robo_ids == crobo_ids )

    def insert_mp_fund_datas_from_file(self,file_name,schema=None):
        '''
            Insert report data to db
        '''
        if schema is None :
            schema = self.schema

        mp = self.read_csv(file_name,dtype=schema.dtype)

        mp = mp.iloc[:,:-1]
        mp.columns = self.schema.columns 
        ret = self.df_to_value_query_string(mp)
        ret.apply(lambda x : self.insert_values("{}_mp_{}".format(self.bank,self.service_type),x))

        self.db.commit()

    def mp_data_frame_to_db(self,df,target_id) :
        '''
            Insert mp file data to db
        '''
        port_s = self.get_port_selector(self.port_num)        
        weight_s = self.get_weight_selector(self.port_num)        

        df["BASC_DT"] = df.index.values

        def sub(x) :
            ports = x[port_s].values
            weights = x[weight_s].values
            
            date = add_colon_if_value_str(x["BASC_DT"]) 
            datas = list(map(lambda f , w , sn : (date,self.model_num,target_id,f,w,sn), ports,weights,range(self.port_num))) 
            df = pd.DataFrame(data=datas)
            
            qdf = self.df_to_value_query_string(df)
            qdf.apply(lambda x : self.insert_values(self.target_table,x))

        df.apply(sub,axis=1)

    def upload_target_mp(self,target_id) :
        self.init_id(target_id)
        target_file = self.get_target_file_name(target_id)

        mp = self.read_csv(target_file,self.csv_schema)
        self.mp_data_frame_to_db(mp,target_id)

        return self.db.commit()

    def get_target_mp_from_file(self,robo_id) :
        self.init_id(robo_id)

        target_file = self.get_target_file_name(robo_id)

        return self.read_csv(target_file,self.csv_schema)

    def getmp(self,target_id,model_num):
        self.init_id(target_id)
        df = self.get_target_mp_from_db_raw_form(target_id,model_num)

        df = df.set_index("BASC_DT")
        df = df.sort_index()

        rebal_dates = set(df.index.values)
        columns = generate_mp_column_name(self.port_num)
    
        ndf = pd.DataFrame(index=rebal_dates,columns=columns)
        pselector = self.get_port_selector(self.port_num)
        wselector = self.get_weight_selector(self.port_num)

        for idx,v in ndf.iterrows() :
            funds = df.loc[idx]["INTT_FUND_CD"].values
            weights = df.loc[idx]["FUND_WEIT"].values

            ndf.loc[idx,pselector] = funds
            ndf.loc[idx,wselector] = weights

        ndf[wselector] = ndf[wselector].astype(float)

        ndf.index.name = "BASC_DT"
        ndf.index = pd.to_datetime(ndf.index)
        ndf = ndf.sort_index()

        return ndf

    def get_target_mp_from_db_raw_form(self,target_id,model_num) :
        
        self.init_id(target_id)

        sql = "select * from {} where ROBO_ID = {} and MODEL_NUM = {} order by BASC_DT,SLOT_NUM;".format(self.target_table,target_id,model_num)
        print(sql)

        self.cursor.execute(sql) 

        mps = self.cursor.fetchall()

        if mps == [] :
            df = pd.DataFrame(columns=["BASC_DT","MODEL_NUM","ROBO_ID","INTT_FUND_CD","FUND_WEIT"])
            return df.set_index("BASC_DT") 

        df = pd.DataFrame.from_dict(mps)
        df["FUND_WEIT"] = df["FUND_WEIT"].astype(float)

        return df

    def init_id(self,robo_id) :
        num = int(robo_id[-1])

        if hasattr(self.schema,"ex_dict") and robo_id in self.schema.ex_dict :
            self.port_num = int(self.schema.ex_dict[robo_id][-1])
        else :
            self.port_num = num

        if hasattr(self.csv_schema,"ex_dict") and robo_id in self.csv_schema.ex_dict :
            self.port_num = int(self.csv_schema.ex_dict[robo_id][-1])

        else :
            self.port_num = num
            
        self.csv_schema.init_robo_id(robo_id)
        self.schema.init_robo_id(robo_id)
        self.robo_id = robo_id

        self.ps = self.get_port_selector(self.port_num)
        self.ws = self.get_weight_selector(self.port_num)

    def get_target_file_name(self,robo_id) :
        return self.dir + "mp_fund_{}_RVN_3M.csv".format(robo_id)

    def get_rebalance_result_file_name(self,robo_id) :
        return qpath.PATH_SAVE_DIR(self.bank,self.service_type) + robo_id + ".csv"

    def insert_rebalance_result(self,robo_id) :
        self.init_id(robo_id)
        f = self.get_rebalance_result_file_name(robo_id)
        df = self.read_csv(f,self.csv_schema)

        assert(df.index.size == 1)

        self.mp_data_frame_to_db(df,robo_id)
        self.db.commit()

    def set_model_num(self,model_num) :
        self.model_num = model_num
        self.dir = env.qrobo_HOME + "\\run_future\\models\\{}\\{}\\model_{}\\".format(self.bank,self.service_type,model_num)

    def raw_form_to_db(self,df,robo_id) :
        self.init_id(robo_id)
        df["BASC_DT"] = df.index.values

        def _task(row) :
            date = row.BASC_DT
            target_id = row.ROBO_ID
            fund = row.INTT_FUND_CD
            weight = row.FUND_INV_WEIT   
            
            datas = ( date ,self.model_num,target_id,fund,weight,_task.slot_num )

            _task.slot_num += 1
            _task.slot_num = _task.slot_num % self.port_num

            values = str(datas)
            sql = "insert into {} values{}".format(self.target_table,values)
            self.cursor.execute(sql)
            print(sql)
            
        _task.slot_num = 0
        df.apply(_task,axis=1)

        return self.db.commit()

