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
        self.chunk_support = self.support / self.partitions

        logger.info(f'Created SON instance with: partitions={self.partitions}, support={self.support}, chunk support={self.chunk_support}')



    def candidate_frequent_itemsets(self):

        # Extract basket support from class (spark doesent like instance attributes)
        chunk_support = self.chunk_support
        data_size = self.data.count()
        # Clean up and extract items in every partition
        baskets = self.data

        # Extract frequent itemsets from every partition (mapreduce 1)
        candidate_frequent_itemsets = (baskets
            .mapPartitions(lambda x: apriori(list(x), chunk_support))   # Applying apriori algorithm on every partition
            .map(lambda x: (x, 1))                                      # Form key-value shape emitting (itemset, 1)
            .groupByKey()                                               # Group every itemset
            .map(lambda x: x[0])                                        # Keep only itemsets
            )

        logger.debug(f'Extracted itemsets: {candidate_frequent_itemsets.collect()}')

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
        logger.debug(f'Found itemsets: {fi}')

        out_fi = []
        # Keep only larger frequent itemsets
        for i in sorted([(i[0], ) if isinstance(i[0], str) else i[0] for i in fi], key=len, reverse=True): 
            if all([not set(i).issubset(set(j)) for j in out_fi]):
                out_fi.append(i)
        
        logger.info(f'Resulting itemsets: {out_fi}')

        return out_fi



