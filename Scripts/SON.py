#from pymongo import MongoClient
from Scripts.apriori import apriori
from Scripts.utils import count_frequencies

import logging
logger = logging.getLogger('son')

class SON:

    def __init__(self, data, support):
        # Input data as pyspark rdd
        self.data = data
        # Reading the number of partitions to properly size everything
        self.partitions = data.getNumPartitions()
        # Reading input support
        self.support = support

        logger.info(f'Created SON instance with: partitions={self.partitions}, support={self.support}')


    def candidate_frequent_itemsets(self):
        # Extract basket support from class (spark doesn't like instance attributes)
        data_size = self.data.count()
        print(f'Number of baskets: {data_size}')
        baskets = self.data
        support = self.support

        # Extract frequent itemsets from every partition (mapreduce 1)
        candidate_frequent_itemsets = (baskets
            .mapPartitions(lambda x: apriori(list(x), support, data_size))      # Applying apriori algorithm on every partition
            .map(lambda x: (x, 1))                                              # Form key-value shape emitting (itemset, 1)
            .groupByKey()                                                       # Group every itemset
            .map(lambda x: x[0])                                                # Keep only itemsets
            ).collect()
        
        candidate_frequent_itemsets = baskets.context.broadcast(candidate_frequent_itemsets)

        # Count the number of baskets containing every itemset (mapreduce 2)
        frequent_itemsets = (baskets
            .mapPartitions(lambda x: count_frequencies(candidate_frequent_itemsets.value, list(x)))     # Count the number of occurences of every itemset
            .reduceByKey(lambda x, y: x + y)                                                            # Sum the number of baskets containing every itemset
            .filter(lambda x: x[1] / data_size >= support)                                              # Filter itemsets with support >= input support
            )

        return frequent_itemsets
