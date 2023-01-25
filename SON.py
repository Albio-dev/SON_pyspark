#from pymongo import MongoClient
from math import floor
from itertools import combinations

class SON:

    def __init__(self, data, support):
        self.data = data
        self.partitions = data.rdd.getNumPartitions()
        self.basket_size = data.count()/self.partitions
        self.support = support
        self.basket_support = self.support / self.partitions



    def candidate_frequent_itemsets(self):

        basket_support = self.basket_support
        basket_sizes = [len(i) for i in self.data.rdd.glom().collect()]

        # Debug print
        print(f'{self.partitions} partizioni\n{basket_support} supporto richiesto in ogni basket\n{self.basket_size} elementi in ogni basket\nChunks sizes: {basket_sizes}')

        # Clean up and extract items in every partition
        baskets = self.data.rdd.mapPartitions(lambda x: [j.good_scores for j in x], preservesPartitioning = True) 
        
        # Use this if we keep the smaller itemsets (in apriori)
        test = (baskets.mapPartitions(lambda x: apriori2(x, basket_support))
            .map(lambda x: (x, 1))
            .groupByKey()
            .map(lambda x: x[0]))

        # [print(f'{i}\n') for i in test.glom().collect()]

        support = self.support
        frequent_itemsets = (test.coalesce(1).glom().cartesian(baskets.glom())
            .mapPartitions(lambda x: list(x)[0])
            .mapPartitions(count_frequencies2)
            .reduceByKey(lambda x, y: x + y)
            .filter(lambda x: x[1] / 980 >= support))

        print(frequent_itemsets.glom().collect())

        # support = self.support
        # test.persist()
        # frequent_itemsets = baskets.mapPartitionsWithIndex(lambda idx, it: count_frequencies2(test.value[idx], it)) \
        #                    .flatMap(lambda x: x) \
        #                    .reduceByKey(lambda x, y: x + y) \
        #                    .filter(lambda x: x[1] >= support)


        # # Print frequent itemsets
        # print(frequent_itemsets.collect())

        # Use this if we keep only the bigger itemsets (in apriori)
        # candidates = baskets.mapPartitions(lambda x: apriori2(x, basket_support)) \
        #     .map(lambda x: (x, 1)) \
        #     .groupByKey() \
        #     .map(lambda x: x[0])

        # # Print candidates
        # [print(f'{i}\n') for i in candidates.glom().collect()]

        # b = baskets.collect()
        # basket_size = self.basket_size
        # support = self.support
        # # Count frequencies
        # frequencies = candidates.mapPartitions(lambda x: count_frequencies2([x, b])) \
        #     .map(lambda x: (x[0], x[1]/basket_size)) \
        #     .filter(lambda x: x[1] >= support)

        # # Print frequent itemsets
        # [print(f'{i}\n') for i in frequencies.glom().collect()]

        # TODO: Mantenere tutti gli itemset che compaiono almeno una volta



def apriori2(basket, basket_e):

    basket = list(basket)
    basket_size = len(basket)
    items = []
    frequent_itemsets = []

    items = list(set([k for j in basket for k in j]))    
    temp = count_frequencies2([items, basket])

    frequent_items = [i[0] for i in temp if i[1]/basket_size >= basket_e]

    new_frequent_itemsets = frequent_items
    while new_frequent_itemsets != []:
        frequent_itemsets += new_frequent_itemsets
        # frequent_itemsets.append(new_frequent_itemsets)
        # frequent_itemsets = new_frequent_itemsets + \
        #     [(i, ) if isinstance(i, str) else i \
        #     for i in frequent_itemsets \
        #         if all( \
        #             [not set([i]).issubset(k) if isinstance(i, str) else not set(i).issubset(k) for k in new_frequent_itemsets] \
        #             )]

        new_candidate_itemsets = [(j, ) + (k, ) if isinstance(j, str) else j + (k,) for j in new_frequent_itemsets for k in frequent_items if k not in j]
        new_candidate_itemsets = [tuple(j) for j in {frozenset(i) for i in new_candidate_itemsets}]
        
        temp = count_frequencies2([new_candidate_itemsets, basket])

        new_frequent_itemsets = [i[0] for i in temp if i[1]/basket_size >= basket_e]

    # print(frequent_itemsets)
    return frequent_itemsets
        
def count_frequencies2(data_chunk):
    itemsets = data_chunk[0]
    data = data_chunk[1]
    values = []
    for i in itemsets:
        count = 0        
        for j in data:
            if type(i) not in [list, tuple, set]:
                if i in j:
                    count += 1
            else:
                if all(items in j for items in i):
                    count += 1
        values.append((i, count))
    return values
