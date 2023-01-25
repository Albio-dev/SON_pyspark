#from pymongo import MongoClient
from math import floor
from itertools import combinations

class SON:

    def __init__(self, data, support):
        # Input data as pyspark rdd
        self.data = data
        # Reading the number of partitions to properly size everything
        self.partitions = data.getNumPartitions()
        # Reading input support
        self.support = support
        # Scaling support for chunks
        self.chunk_support = self.support / self.partitions



    def candidate_frequent_itemsets(self):

        # Extract basket support from class (spark doesent like instance attributes)
        chunk_support = self.chunk_support
        data_size = self.data.count()
        # Clean up and extract items in every partition
        baskets = self.data

        # Debug print
        print(f'{self.partitions} partizioni\n{chunk_support} supporto richiesto in ogni basket')#\nChunks sizes: {basket_sizes}')

        # Extract frequent itemsets from every partition (mapreduce 1)
        candidate_frequent_itemsets = (baskets
            .mapPartitions(lambda x: apriori(list(x), chunk_support))   # Applying apriori algorithm on every partition
            .map(lambda x: (x, 1))                                      # Form key-value shape emitting (itemset, 1)
            .groupByKey()                                               # Group every itemset
            .map(lambda x: x[0])                                        # Keep only itemsets
            )

        # Check for false positive/false negative (mapreduce 2)
        support = self.support
        frequent_itemsets = (candidate_frequent_itemsets
            .repartition(1)                                             # Set candidates as a single partition
            .glom()                                                     # Group partition elements together
            .cartesian(baskets.glom())                                  # Distribute to every basket partition
            .mapPartitions(lambda x : count_frequencies(list(x)[0]))    # Count candidates absolute frequencies for every batch
            .reduceByKey(lambda x, y: x + y)                            # Sum absolute frequencies for every candidate itemset
            .filter(lambda x: x[1] / data_size >= support)              # Filter out candidates with less than required support
            )

        fi = frequent_itemsets.collect()
        out_fi = []

        print(fi)

        # Keep only larger frequent itemsets
        for i in sorted([(i[0], ) if isinstance(i[0], str) else i[0] for i in fi], key=len, reverse=True): 
            if all([not set(i).issubset(set(j)) for j in out_fi]):
                out_fi.append(i)

        print(out_fi)




# Apriori algorithm. Requires data as list and accepted support
def apriori(data, support):
    # Get actual batch size
    basket_size = len(data)
    # Prepare output structure
    frequent_itemsets = []
    # Extract item list
    items = list(set([k for j in data for k in j]))
    # Use function to get items frequencies in batch
    temp = count_frequencies((items, data))
    # Filter only frequent ones and put singlets in tuples
    frequent_items = [i[0] for i in temp if i[1]/basket_size >= support]
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
        temp = count_frequencies((new_candidate_itemsets, data))
        # Filter out non-frequent itemsets
        new_frequent_itemsets = [i[0] for i in temp if i[1]/basket_size >= support]
        
    return frequent_itemsets
        
# Utility function to count occurrences of items in the first list inside elements of the second list
# e.g.: ( [itemsets], [baskets] )
def count_frequencies(data_chunk):
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
