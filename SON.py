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
        # Apriori on batch
        #x = self.data.rdd.mapPartitions(lambda x: [i.good_scores for i in x]) \
        #    .mapPartitions(apriori).collect()
        # x = self.data.rdd.mapPartitions(self.apriori).collect()#.reduceByKey(lambda x, y: x + y).filter(lambda x: x[1] >= self.basket_support).collect()
        #print(x)
        # Emit fi

        basket_support = self.basket_support
        #basket_size = self.basket_size
        basket_sizes = [len(i) for i in self.data.rdd.glom().collect()]

        print(f'{self.partitions} partizioni\n{basket_support} supporto richiesto in ogni basket\n{self.basket_size} elementi in ogni basket')

        # Clean up and extract items in every partition
        baskets = self.data.rdd.mapPartitions(lambda x: [j.good_scores for j in x], preservesPartitioning = True) \
            .mapPartitionsWithIndex(lambda i, x: [list(set([k for j in x for k in j])), x, basket_sizes[i]], True)
        
        # Count and extract items appearances in each basket
        frequent_itemsets_singlets = baskets.mapPartitions(count_frequencies, True) \
            .filter(lambda x: x[1]/x[2] >= basket_support) \
            .keys()

        # Save first pass of apriori
        frequent_itemsets = frequent_itemsets_singlets.glom().collect()
        # Save singlets for future expansions
        frequent_items = frequent_itemsets

        # Prepare baskets for next round of counting by stripping them of items and adding partition index
        baskets = baskets.mapPartitions(lambda x: [x[1], x[2]])

        # TODO: Vedere se il ciclo può cominciare da qui

        # Prepare new itemsets as FI x FI
        new_candidates = frequent_itemsets_singlets.mapPartitions(lambda x: list(combinations(x, 2)))

        # Combine structures per partition and clean of indexes
        #new_structure = new_candidates.zip(baskets).mapPartitions(lambda x: list(x)[1], True)
        new_structure = new_candidates.glom().zip(baskets.glom()) \
            .mapPartitions(lambda x: (y:=list(x)[0], [y[0], y[1][0], y[1][1]])[-1], True)

        # Again, count
        new_frequent_itemsets = new_structure.mapPartitions(count_frequencies, True) \
            .filter(lambda x: x[1]/x[2] >= basket_support) \
            .keys() 

        while not new_frequent_itemsets.isEmpty():
            frequent_items = [i + j for i, j in zip(frequent_items, new_frequent_itemsets.glom().collect())]
            # TODO: Filtrare e tenere solo quelli massimali

            # Generate new candidate itemsets starting from frequent ones
            new_candidate_itemsets = new_frequent_itemsets.glom() \
                .zip(frequent_itemsets_singlets.glom()) \
                .glom() \
                .mapPartitions(lambda x: list(x)[0][0], True) \
                .mapPartitions(lambda x: [j + (k, ) if isinstance(k, str) else j + k for j in list(x)[0] for k in list(x)[1] if k not in j], True) \
                .mapPartitions(lambda x: [tuple(j) for j in {frozenset(i) for i in x}])
                #.mapPartitions(lambda x: [list(j) + [k] for j in list(x)[0] for k in list(x)[1] if k not in j])

            new_structure = new_candidate_itemsets.glom().zip(baskets.glom()).mapPartitions(lambda x: (y:=list(x)[0], [y[0], y[1][0], y[1][1]])[-1], True)

            new_frequent_itemsets = new_structure.mapPartitions(count_frequencies, True) \
                .filter(lambda x: x[1]/x[2] >= basket_support) \
                .keys()

        print(frequent_items)

        # TODO: Va bene sequenziale (singola macchina) o è meglio farlo fare a spark??
        candidate_itemsets = set()
        for i in frequent_items:
            candidate_itemsets = candidate_itemsets.union(i)

        

        

def count_frequencies(data_chunk):
    itemsets = data_chunk[0]
    data = data_chunk[1]
    values = []        
    #print(f'In itemset {itemsets}')
    for i in itemsets:
        #print(f'Item: {i}')
        count = 0        
        for j in data:
            #print(f'basket: {j}')
            if type(i) not in [list, tuple, set]:
                if i in j:
                    count += 1
            else:
                if all(items in j for items in i):
                    count += 1
        values.append((i, count, data_chunk[2]))
    #print(f'{values}\n\n\n')
    return values



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