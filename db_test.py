cd /D "F:\Albio Cloud\Scuola\Magistrale\(2022-2023) Big data\Progetto\Frequent_itemset_spark"
python

from pyspark import SparkContext, SparkConf
from pyspark.sql import SparkSession
import logging
import sys
import lib.SON
import lib.preprocessing
import time 
from lib.apriori import apriori
from lib.utils import count_frequencies

datasets = {0: lib.preprocessing.tripadvisor_review, 1:lib.preprocessing.online_retail}

partitions = 1
selectedDataset = 0
support = 0.75

db_addr = '127.0.0.1'
port = '27017'
datasets = {0: 'TravelReviews.reviews', 1:'OnlineRetail.transactions'}

# Create spark session. Get mongo connector and configure spark
spark = (SparkSession.builder
    .master("local[*]")
    .config('spark.executor.memory', '2g')
    .config('spark.driver.memory', '4g')
    .config('spark.jars.packages', 'org.mongodb.spark:mongo-spark-connector:10.0.2')
    .config("spark.mongodb.read.connection.uri", f"mongodb://{db_addr}:{port}/{datasets[selectedDataset]}")
    .config("spark.mongodb.write.connection.uri", f"mongodb://{db_addr}:{port}/{datasets[selectedDataset]}")
    .getOrCreate()
    )

raw_data = spark.read.format("mongodb").load()
data = raw_data.rdd.repartition(partitions)#.mapPartitions(lambda x: [j.items for j in x])


data_size = data.count()
baskets = data.map(set)
set_data = baskets.collect()
raw_data = data.collect()

a = time.time()
test = data.collect()
b = time.time()
print(f'Collect baskets: {b-a}s')

a = time.time()
sgvrhd = data.mapPartitions(lambda x: list(i for i in x)).collect()
b = time.time()
print(f'Transform baskets: {b-a}s')
a = time.time()
sgvrhd = data.mapPartitions(lambda x: list(x)).collect()
b = time.time()
print(f'Transform baskets: {b-a}s')





a = time.time()
candidate_frequent_itemsets = (baskets
    .mapPartitions(lambda x: apriori(list(i for i in x), support, data_size))      # Applying apriori algorithm on every partition
    ).collect()
b = time.time()
print(f'SON 1st mapreduce: {b-a}s')

a = time.time()
candidate_frequent_itemsets2 = apriori(set_data, support, data_size)
b = time.time()
print(f'Apriori set: {b-a}s')

a = time.time()
candidate_frequent_itemsets3 = apriori(raw_data, support, data_size)
b = time.time()
print(f'Apriori list: {b-a}s')

candidate_frequent_itemsets == candidate_frequent_itemsets2
candidate_frequent_itemsets == candidate_frequent_itemsets3
candidate_frequent_itemsets2 == candidate_frequent_itemsets3

a = time.time()
candidate_frequent_itemsets = list(set(candidate_frequent_itemsets))
candidate_frequent_itemsets = baskets.context.broadcast(candidate_frequent_itemsets)

frequent_itemsets = (baskets
    .mapPartitions(lambda x: count_frequencies(candidate_frequent_itemsets.value, list(x)))     # Count the number of occurences of every itemset
    .reduceByKey(lambda x, y: x + y)                                                            # Sum the number of baskets containing every itemset
    .filter(lambda x: x[1] / data_size >= support)                                              # Filter itemsets with support >= input support
    )
temp = frequent_itemsets.collect()
b = time.time()
print(f'SON 2nd mapreduce: {b-a}s')