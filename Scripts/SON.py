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
        # Scaling support for chunks
        # TODO: this is not correct, we should scale the support for the number of baskets in every partition
        self.chunk_support = self.support / self.partitions

        logger.info(f'Created SON instance with: partitions={self.partitions}, support={self.support}, chunk support={self.chunk_support}')



    def candidate_frequent_itemsets(self):

        # Extract basket support from class (spark doesent like instance attributes)
        chunk_support = self.chunk_support
        # TODO: can we make a query to the database to get the number of baskets, instead of counting them here (faster?)
        data_size = self.data.count()
        print(f'Number of baskets: {data_size}')
        # Clean up and extract items in every partition
        baskets = self.data

        support = self.support
        # Extract frequent itemsets from every partition (mapreduce 1)
        candidate_frequent_itemsets = (baskets
            .mapPartitions(lambda x: apriori(list(x), chunk_support))   # Applying apriori algorithm on every partition
            .map(lambda x: (x, 1))                                      # Form key-value shape emitting (itemset, 1)
            .groupByKey()                                               # Group every itemset
            .map(lambda x: x[0])                                        # Keep only itemsets
            ).collect()
        
        candidate_frequent_itemsets = baskets.context.broadcast(candidate_frequent_itemsets)

        # Count the number of baskets containing every itemset (mapreduce 2)
        frequent_itemsets = (baskets
            .mapPartitions(lambda x: count_frequencies([candidate_frequent_itemsets.value, list(x)]))   # Count the number of occurences of every itemset
            .reduceByKey(lambda x, y: x + y)                                                            # Sum the number of baskets containing every itemset
            .filter(lambda x: x[1] / data_size >= support))                                             # Filter itemsets with support >= input support

        return frequent_itemsets

