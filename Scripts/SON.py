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

        # Print number of elements in every partition
        # logger.debug(f'Number of elements in every partition: {data.glom().map(len).collect()}')

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
            ).collect()#.cache()

        frequent_itemsetss = baskets.mapPartitions(lambda x: count_frequencies([candidate_frequent_itemsets, list(x)]))
        frequent_itemsetss = frequent_itemsetss.reduceByKey(lambda x, y: x + y)
        frequent_itemsetss = frequent_itemsetss.filter(lambda x: x[1] / data_size >= support)
        # #frequent_itemsetss = frequent_itemsetss.map(lambda x: x[0])
        # #frequent_itemsetss = frequent_itemsetss.collect()

        return frequent_itemsetss



        # return candidate_frequent_itemsets.collect()

        #logger.debug(f'Extracted itemsets: {candidate_frequent_itemsets.collect()}')

        partitions = self.partitions
        # Check for false positive/false negative (mapreduce 2)
        # support = self.support
        # frequent_itemsets = (candidate_frequent_itemsets
        #     .repartition(1)                                                # Set candidates as a single partition
        #     .glom()                                                        # Group partition elements together
        #     .cartesian(baskets.glom())                                     # Distribute to every basket partition
        #     .mapPartitions(lambda x : count_frequencies(list(x)[0]))    # Count candidates absolute frequencies for every batch
        #     .reduceByKey(lambda x, y: x + y)                            # Sum absolute frequencies for every candidate itemset
        #     .filter(lambda x: x[1] / data_size >= support)              # Filter out candidates with less than required support
        #     )
        # print(baskets.glom().collect())

        

        # Return frequent itemsets
        # return frequent_itemsets#.collect()

        # fi = frequent_itemsets.collect()
        # logger.debug(f'Found itemsets: {fi}')
        # return fi

        # fi = candidate_frequent_itemsets.collect()

        # out_fi = []
        # # Keep only larger frequent itemsets
        # for i in sorted([(i[0], ) if isinstance(i[0], str) else i[0] for i in fi], key=len, reverse=True): 
        #     if all([not set(i).issubset(set(j)) for j in out_fi]):
        #         out_fi.append(i)
        
        # logger.info(f'Resulting itemsets: {out_fi}')

        # return out_fi



