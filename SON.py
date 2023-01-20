#from pymongo import MongoClient
from math import floor
from itertools import combinations

class SON:

    def __init__(self, data, partitions, support):
        self.data = data
        self.partitions = partitions
        self.support = support
        self.basket_support = self.support / self.partitions


    def candidate_frequent_itemsets(self):
        # Apriori on batch
        x = self.data.rdd.mapPartitions(lambda x: [i.good_scores for i in x]) \
            .mapPartitions(apriori).collect()
        # x = self.data.rdd.mapPartitions(self.apriori).collect()#.reduceByKey(lambda x, y: x + y).filter(lambda x: x[1] >= self.basket_support).collect()
        print(x)
        # Emit fi


def apriori(basket):
    # Translate items to numbers
    # Create a dictionary with the items and their corresponding number
    items = {}
    index = 0
    # List of counts for each item
    # The i-th element of the list is the count of the i-th item
    counts = []
    for s in basket:
        for i in s:
            if i not in items:
                items[i] = index
                index += 1
                counts.append(0)
            counts[items[i]] += 1

    # Print the frequent items
    # for i in items:
    #     if counts[items[i]] >= support:
    #         print(i, counts[items[i]])

    # Remove the infrequent items
    items = {i: items[i] for i in items if counts[items[i]] >= 2}
    counts = [counts[i] for i in items.values()]

    # Second pass

    # Create a list of all possible pairs of items
    # TODO: is there a better way to do this (without creating all the possible pairs)?
    pairs = list(combinations(items.keys(), 2))
    # print(pairs)

    # List of counts for each pair
    # The i-th element of the list is the count of the i-th pair
    counts_pairs = [0] * len(pairs)
    # print(items)

    # Count the number of times each pair appears
    for s in basket:
        for p in pairs:
            if p[0] in s and p[1] in s:
                counts_pairs[pairs.index(p)] += 1

    # Print the frequent pairs
    # for p in pairs:
    #     if counts_pairs[pairs.index(p)] >= 2:
    #         print(p, counts_pairs[pairs.index(p)])


    return [(p, counts_pairs[pairs.index(p)]) for p in pairs if counts_pairs[pairs.index(p)] >= 2]