import operator
import sys

from nqrobo.layers.meta import meta

class baseSelector :

    def __init__(self,num_select=1) :
        self.num_select = num_select

    def run(self,**input_dict) :
        sets = input_dict["score_sets"]
        sorted_sets = sorted(sets.items(),key=operator.itemgetter(1))

        fkey = sorted_sets[-1][0]
            
        #TODO num_select 에 따라 복수개 선택해서
        # 다음레이어에 스크리너나 score accumulator 추가해서 
        # 자유롭게 모델 구성할수 있게
        input_dict["selected_set"] = meta.hash_id_to_fund_set(fkey)
        input_dict["selected_weight"] = input_dict["weight_sets"][fkey]

        return input_dict

    def set_num_select(self,num) :
        self.num_select = num