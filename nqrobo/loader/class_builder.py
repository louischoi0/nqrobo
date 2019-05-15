import nqrobo.loader.mpsql as mp
import nqrobo.loader.tssql as ts 
import nqrobo.loader.id_generator as ig 

import stringcase as sc

from nqrobo.layers.selector import baseSelector
from nqrobo.layers.screener import baseScreener
from nqrobo.layers.allocator import baseAllocator
from nqrobo.layers.optimizer import baseOptimizer
from nqrobo.layers.feeder import baseFeeder

from collections import deque

PC = sc.pascalcase

class idictBinder :

    @staticmethod
    def bind(instance,idict,*tcls) :
        for c in tcls :
            tc = idict[c]
            setattr(instance,c,tc)

class baseLayerBuilder :

    vdict = {
        "feeder" : baseFeeder,
        "screener" : baseScreener,
        "allocator" : baseAllocator,
        "optimizer" : baseOptimizer,
        "selector" : baseSelector
    }

    @staticmethod
    def build_layer() :
        layer = deque()
        layer_dict = {}

        for key in baseLayerBuilder.vdict :
            icls = baseLayerBuilder.get(key)()
            layer.append(icls)
            layer_dict[key] = icls

        return layer , layer_dict

    @staticmethod
    def get(tcls) :
        return baseLayerBuilder.vdict[tcls]

    @staticmethod
    def bind(instance,*tcls) :
        for t in tcls :
            clss = baseLayerBuilder.get(t)
            setattr(instance,t,clss)

class classBuilder :

    vdict = {
        "mp_loader" : [ "{}MpFund{}Sql" , mp ],
        "mp_ts_loader" : [ "{}MpTsSql{}", ts ],
        "id_generator" : [ "{}{}IdGenerator", ig ],
        "robo_ids" : lambda b , s : getattr(ig,"{}{}IdGenerator".format(b,s))().robo_ids,
    }

    replace_table_all = {}
    replace_table_spc = {}

    @staticmethod
    def replace_service_tag(bank,service,clss) :
        rp_table = classBuilder.replace_table_all
        rp_table_spc = classBuilder.replace_table_spc
        
        if service in rp_table :
            return rp_table[service]

        if bank in rp_table_spc and clss in rp_table_spc[bank] :

            if service in rp_table_spc[bank][clss] :
                return rp_table_spc[bank][clss][service]

        return service

    @staticmethod
    def get(bank,service_type,tcls) :

        holder = classBuilder.vdict[tcls]
        _service_type = sc.pascalcase(service_type)
        _service_type = classBuilder.replace_service_tag(bank,_service_type,tcls)

        if type(holder) == type(lambda x,y : x + y)  :
            return holder(bank,_service_type)

        class_name = holder[0].format(bank,_service_type)
        return getattr(holder[1],class_name)()

    @staticmethod
    def bind(instance,bank,service_type,*classes) :
        '''
            bank,service_type 에 따라 필요한 classes 모듈들을 인스턴스에
            바인드 해주는 함수
        '''
        for c in classes :
            class_instance = classBuilder.get(bank,service_type,c)
            setattr(instance,c,class_instance)