#from pymongo import MongoClient
from math import floor
from pyspark.sql import SparkSession

from itertools import combinations

import SON

db_addr = '127.0.0.1'
forcePartitions = 10

spark = (SparkSession.builder
    .config('spark.jars.packages', 'org.mongodb.spark:mongo-spark-connector_2.12:3.0.1') \
    .config("spark.mongodb.input.uri", f"mongodb://{db_addr}/TravelReviews.reviews") \
    .config("spark.mongodb.output.uri", f"mongodb://{db_addr}/TravelReviews.reviews") \
    .config("spark.mongodb.input.partitioner", "MongoPaginateByCountPartitioner") \
    .config("spark.mongodb.input.partitionerOptions.partitionKey", "_id") \
    .config("spark.mongodb.input.partitionerOptions.numberOfPartitions", "10") \
    .getOrCreate()
    )
input_data = spark.read.format("mongo").load()

print("\n\n")
print(f'Settings partition value:\t\t{spark.conf.get("spark.mongodb.input.partitionerOptions.numberOfPartitions")}')
print(f'Automatically partitioned value: \t{input_data.rdd.getNumPartitions()}')
input_data = input_data.coalesce(forcePartitions)
print(f'Partitions after forcing {forcePartitions}: \t\t{input_data.rdd.getNumPartitions()}')

data = input_data.rdd.mapPartitions(lambda x: [j.good_scores for j in x])

# Preprocessing: trasformare gli item in numeri
#items = {i: items[i] for i in items if counts[items[i]] >= support}




epsilon = .85

son = SON.SON(data, epsilon)
son.candidate_frequent_itemsets()