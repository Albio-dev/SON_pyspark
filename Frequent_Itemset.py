#from pymongo import MongoClient
from math import floor
from pyspark.sql import SparkSession

from itertools import combinations

import SON

db_addr = '127.0.0.1'

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
print(data)
print(spark.conf.get("spark.mongodb.input.partitionerOptions.numberOfPartitions"))



epsilon = .5
p = .1

print(data.rdd.getNumPartitions())
data = data.coalesce(10)
print(data.rdd.getNumPartitions())

son = SON.SON(data, 10, 100)
son.candidate_frequent_itemsets()
