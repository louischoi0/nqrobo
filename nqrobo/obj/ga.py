import numpy as np

def run_ga(obj,ts,fi,consts) :
    return GA(ts,obj,consts).run()

class GA :
    def __init__(self, stime_series, obj,consts, init_value=None, 
                 mutate_rate=0.05, generations=10 ,instancies=100, to_mutate=6, to_crossover=6, **kwargs):
        self.instancies = None
        self.mutate_rate = mutate_rate
        self.obj = obj

        self.vsize = stime_series.columns.size

        self.to_mutate = to_mutate
        self.to_crossover = to_crossover

        self.t_instance = instancies
        self.scores = None

        self.generations = generations
        self.const_functions = consts

        init_value = init_value if init_value is not None else np.array([1/self.vsize] * self.vsize)

        self.initialize(init_value)

    def check_consts(self) :
        for idx,w in enumerate(self.instancies) :
            for const_func in self.const_functions :
                if not const_func(w) :
                    self.scores[idx] = -100
                    break

    def evaluate(self):
        self.scores = np.apply_along_axis(self.obj, 1, self.instancies)
        self.check_consts()

    def initialize(self,init_value):
        self.instancies = map(lambda x: init_value,range(self.t_instance))
        self.instancies = np.array(list(self.instancies))
        self.evaluate()

    def mutate(self):
        to_mutate = self.to_mutate
        mutate_rate = self.mutate_rate

        midx = np.random.randint(self.t_instance,size=(to_mutate))
        eidx = np.random.randint(self.vsize,size=(to_mutate,2))

        mrate = np.random.uniform( 1 - (mutate_rate / 2), 1 + (mutate_rate / 2) , size=(to_mutate) )

        for idx, vidx in enumerate(midx) :
            eeidx = eidx[idx]

            if eeidx[0] == eeidx[1] :
                continue

            target = self.instancies[vidx][eeidx[0]]
            diff = target * mrate[idx] - target

            self.instancies[vidx][eeidx[0]] += diff
            self.instancies[vidx][eeidx[1]] -= diff

    def cross_over(self):
        to_crossover = self.to_crossover
        sorted_idx = np.argsort(self.scores)

        targets = self.pick_dead(sorted_idx,self.to_crossover)
        parents = self.pick_parents(sorted_idx,self.to_crossover)

        eidx = np.random.randint(self.vsize,size=(to_crossover,2))
        parents_idx = np.random.randint(len(parents),size=(to_crossover,2))

        for idx,vidx in enumerate(targets) :
            elems = np.array(eidx[idx])

            mom = self.instancies[parents_idx[idx][0]]
            dad = self.instancies[parents_idx[idx][1]]

            self.instancies[vidx][elems] = mom[elems]
            self.instancies[vidx][~elems] = dad[~elems]

    def pick_parents(self,sorted_idx,to_pick):
        return sorted_idx[-to_pick:]

    def pick_dead(self,sorted_idx,to_pick):
        return sorted_idx[:to_pick]

    def run(self):
        for _ in range(self.generations) :
            self.mutate()
            self.instancies = np.apply_along_axis(lambda x : GA.round_weights(x),1,self.instancies)

            self.evaluate()
            self.cross_over()

            self.instancies = np.apply_along_axis(lambda x : GA.round_weights(x), 1, self.instancies)
            self.evaluate()

        sorted_idx = np.argsort(self.scores)
        best = sorted_idx[-1]

        print(self.scores[best])
        print(self.instancies[best])

        return self.scores[best], self.instancies[best]

    @staticmethod
    def round_weights(weights):
        weights = weights / np.sum(weights)

        _floating = map(lambda x : (x * 100 ) % 1 , weights)
        _floating = list(_floating)

        to_round = int( np.sum(_floating) + 0.1)
        rounded = 0

        _sorted = np.argsort(_floating)
        _sorted = np.flip(_sorted,axis=0)

        for idx in _sorted :
            if rounded < to_round :
                weights[idx] = int(weights[idx] * 100 ) / 100
                weights[idx] += 0.01
                rounded += 1

            else :
                weights[idx] = int(weights[idx] * 100) / 100

        return weights


