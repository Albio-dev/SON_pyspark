from random import choice as random_choice
from pandas import DataFrame as Table
from functools import reduce
from collections import Counter as CounterDictionary

def support(td, tr):
	return reduce(lambda x, y: x & y, [td[it] for it in tr]).mean()

def next_candidates(itemsets):
	if len(itemsets) == 0: return set()
	r = set()
	k = len(list(itemsets)[0])
	for its in itemsets:
		nns = set(filter(lambda x: len(its.difference(x)) == 1, itemsets)) # nns: nearest neighbours
		nn_diff = [list(nn.difference(its))[0] for nn in nns]
		nn_diff_count = CounterDictionary(nn_diff).items()
		extensions = frozenset([frozenset([pair[0]]) for pair in list(filter(lambda pair: pair[1] == k, nn_diff_count))])
		for singleton in extensions: r.add( frozenset(its.union(singleton)))
	return r
	
def apriori(td, epsilon):
	fi = set()
	items = set(td.columns)
	candidates = set([frozenset([it]) for it in items])
	condition = True
	while(condition):
		fi_k = set( filter(lambda x: support(td, x) >= epsilon , candidates) )
		fi = fi.union(fi_k)
		candidates = next_candidates(fi_k)
		condition = len(candidates) > 0
	return fi	