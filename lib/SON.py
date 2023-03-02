from lib.apriori import apriori
from lib.apriori import apriori2
from lib.utils import count_frequencies

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

    # This function is used to extract the candidate frequent itemsets
    # Returns a pyspark rdd with the candidate frequent itemsets
    # Returns None if the dataset is too small to produce frequent itemsets with the given support
    def candidate_frequent_itemsets(self):
        # Extract basket support from class (spark doesn't like instance attributes)
        data_size = self.data.count()
        # print(f'Number of baskets: {data_size}')
        baskets = self.data
        support = self.support

        # Extract frequent itemsets from every partition (mapreduce 1)
        candidate_frequent_itemsets = (baskets
            .mapPartitions(lambda x: apriori2(list(x), support, data_size))      # Applying apriori algorithm on every partition
            ).collect()
        
        candidate_frequent_itemsets = list(set(candidate_frequent_itemsets))
        if None in candidate_frequent_itemsets:
            print(f'None in candidate_frequent_itemsets')
            return None
        candidate_frequent_itemsets = baskets.context.broadcast(candidate_frequent_itemsets)

        # Count the number of baskets containing every itemset (mapreduce 2)
        frequent_itemsets = (baskets
            .mapPartitions(lambda x: count_frequencies(candidate_frequent_itemsets.value, list(x)))     # Count the number of occurences of every itemset
            .reduceByKey(lambda x, y: x + y)                                                            # Sum the number of baskets containing every itemset
            .filter(lambda x: x[1] / data_size >= support)                                              # Filter itemsets with support >= input support
            )

        return frequent_itemsets
