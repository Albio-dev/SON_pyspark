from collections import Counter as CounterDictionary

def top(l , limit=None):
	r = list(CounterDictionary(l).items())
	r.sort(key=lambda x: x[1], reverse=True)
	return [x[0] for x in r[0:limit]]