#from pymongo import MongoClient
from math import floor
from pyspark.sql import SparkSession

from itertools import combinations

import SON

db_addr = '127.0.0.1'
forcePartitions = 10

spark = SparkSession.builder.config('spark.jars.packages', 'org.mongodb.spark:mongo-spark-connector_2.12:3.0.1') \
    .config("spark.mongodb.input.uri", "mongodb://127.0.0.1/TravelReviews.reviews") \
    .config("spark.mongodb.output.uri", "mongodb://127.0.0.1/TravelReviews.reviews") \
    .config("spark.mongodb.input.partitioner", "MongoPaginateByCountPartitioner") \
    .config("spark.mongodb.input.partitionerOptions.partitionKey", "_id") \
    .config("spark.mongodb.input.partitionerOptions.numberOfPartitions", "10") \
    .getOrCreate()
#print(spark)
data = spark.read.format("mongo").load()

print("\n\n")
print(f'Settings partition value:\t\t{spark.conf.get("spark.mongodb.input.partitionerOptions.numberOfPartitions")}')
print(f'Automatically partitioned value: \t{data.rdd.getNumPartitions()}')
data = data.coalesce(forcePartitions)
print(f'Partitions after forcing {forcePartitions}: \t\t{data.rdd.getNumPartitions()}')


epsilon = .5

son = SON.SON(data, epsilon)
son.candidate_frequent_itemsets()
