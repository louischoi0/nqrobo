
from nqrobo.loader.class_builder import baseLayerBuilder
import nqrobo.callback.functions as cb

class roboObject :
    def __init__(self) :
        self.layers , self.ldict = baseLayerBuilder.build_layer()
        self.inference() 

    def run(self,**input_dict) :

        for it in self.layers :
            input_dict = it.run(**input_dict)

        self.fdict = input_dict

        return input_dict

    def run_id(self,robo_id,**input_dict) :
        input_dict["target_id"] = robo_id
        return self.run(**input_dict)

    def inference(self) :
        pass

class ibkBasicObject(roboObject) :
    def __init__(self) :
        super(ibkBasicObject,self).__init__()

    def inference(self) :
        print(self.layers)
        fi,sc,al,opt,sel = self.layers

        sc.add_fund_types("dstock","stock","bond","fstock","dbond")
        sc.set_num_selection({"dstock":2,"stock":1,"bond":2,"dbond":1})

        sc.add_filter("stock", { "type" : "rvn", "thres" :  0.4 , "delta" : 30 , "eval" : "rel"  })
        sc.add_filter("dstock", { "type" : "rvn", "thres" :  0.4 , "delta" : 30 , "eval" : "rel"  })
        sc.add_filter("bond", { "type" : "rvn", "thres" :  0.4 , "delta" : 30 , "eval" : "rel"  })

        sc.set_eval_func("mom")

        al.add_type_forms("1104","stock","dstock","bond","bond")
        al.add_type_forms("1204","dstock","dstock","dbond","dbond")



        al.set_default_form("bond","stock","stock","dstock")

        opt.set_eval_func("mom_ga")

    def test(self) :
        sets = self.fdict["selected_set"]
        weights = self.fdict["selected_weight"]
        fi = self.fdict["fund_pool"]

        print(fi.loc[sets,:])
        print(weights)



        