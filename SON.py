#from pymongo import MongoClient
from math import floor
from itertools import combinations

class SON:

    def __init__(self, data, support):
        # Input data as pyspark Dataframe
        self.data = data
        # Reading the number of partitions to properly size everything
        self.partitions = data.rdd.getNumPartitions()
        # Counting how many items are in a chunk (TODO: useless?)
        self.basket_size = data.count()/self.partitions
        # Reading input support
        self.support = support
        # Scaling support for chunks
        self.basket_support = self.support / self.partitions



    def candidate_frequent_itemsets(self):

        # Extract basket support from class (spark doesent like instance attributes)
        basket_support = self.basket_support
        # Calculating exactly partitions sizes
        basket_sizes = [len(i) for i in self.data.rdd.glom().collect()]

        # Debug print
        print(f'{self.partitions} partizioni\n{basket_support} supporto richiesto in ogni basket\n{self.basket_size} elementi in ogni basket\nChunks sizes: {basket_sizes}')

        # Clean up and extract items in every partition
        baskets = self.data.rdd.mapPartitions(lambda x: [j.good_scores for j in x], preservesPartitioning = True) # Extract column from every row in partition

        # Extract frequent itemsets from every partition
        frequent_itemsets_partitions = (baskets
            .mapPartitions(lambda x: apriori2(x, basket_support))   # Applying apriori algorithm on every partition
            .map(lambda x: (x, 1))                     # Form key-value shape emitting (itemset, 1)
            )
        
        # Extract every unique itemsets produced in any partition
        candidate_frequent_itemsets = (frequent_itemsets_partitions
            .groupByKey()           # Group by itemset
            .map(lambda x: x[0])    # Keep only keys (itemsets)
            )


        [print(f'{i}\n') for i in test.glom().collect()]




def apriori2(basket, basket_e):
    # Basket is passed as an iterator, so we convert it in list
    basket = list(basket)
    # Get actual batch size
    basket_size = len(basket)
    frequent_itemsets = []
    # Extract item list
    items = list(set([k for j in basket for k in j]))
    # Use function to get items frequencies in batch
    temp = count_frequencies2([items, basket])
    # Filter only frequent ones and put singlets in tuples
    frequent_items = [i[0] for i in temp if i[1]/basket_size >= basket_e]
    # Check if any is produced
    new_frequent_itemsets = frequent_items
    while new_frequent_itemsets != []:
        # If so, store them
        frequent_itemsets += new_frequent_itemsets
        # Keep only largest sets
        '''frequent_itemsets = new_frequent_itemsets + \
            [(i, ) if isinstance(i, str) else i \
            for i in frequent_itemsets \
                if all( \
                    [not set([i]).issubset(k) if isinstance(i, str) else not set(i).issubset(k) for k in new_frequent_itemsets] \
                    )]'''
        # Form new candidates appending singlets to the last frequent itemsets
        new_candidate_itemsets = [(j, ) + (k, ) if isinstance(j, str) else j + (k,) for j in new_frequent_itemsets for k in frequent_items if k not in j]
        # Filter out repeated elements (not considering order)
        new_candidate_itemsets = [tuple(j) for j in {frozenset(i) for i in new_candidate_itemsets}]        
        # Count occurrences of the new itemset
        temp = count_frequencies2([new_candidate_itemsets, basket])
        # Filter out non-frequent itemsets
        new_frequent_itemsets = [i[0] for i in temp if i[1]/basket_size >= basket_e]
    return frequent_itemsets
        
def count_frequencies2(data_chunk):
    # Itemsets to check
    itemsets = data_chunk[0]
    # Batch to count in itemsets in
    data = data_chunk[1]
    values = []
    # For every itemset
    for i in itemsets:
        count = 0        
        # Check if it exists in every basket
        for j in data:
            # This check is to handle strings
            if type(i) not in [list, tuple, set]:
                if i in j:
                    count += 1
            else:
                if all(items in j for items in i):
                    count += 1
        values.append((i, count))
    return values
