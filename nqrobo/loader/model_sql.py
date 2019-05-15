from nqrobo.loader.sql_base import *

class mpModelSql(sqlBase) :
    def __init__(self) :
        self.db = get_connection("robo_advisors")
        self.cursor = self.db.cursor()
        self.model_num_table = "model_log"

    def load_model_num(self,bank,service) :
        sql = "select MODEL_NUM from {} where BANK = \'{}\' and SERVICE_TYPE = \'{}\'".format(self.model_num_table,bank,service)
        self.cursor.execute(sql)
        nmodel = self.cursor.fetchone()["MODEL_NUM"]

        print( "{} {} model_{}".format(bank,service,nmodel))
        return "model_{}".format(str(nmodel))
