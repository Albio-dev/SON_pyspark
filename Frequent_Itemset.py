from pymongo import MongoClient
from math import floor
from pyspark.sql import SparkSession

db_addr = '127.0.0.1'

spark = SparkSession.builder.config('spark.jars.packages', 'org.mongodb.spark:mongo-spark-connector_2.12:3.0.1') \
    .config("spark.mongodb.input.uri", "mongodb://127.0.0.1/TravelReviews.reviews") \
    .config("spark.mongodb.output.uri", "mongodb://127.0.0.1/TravelReviews.reviews") \
    .getOrCreate()
#print(spark)
data = spark.read.format("mongo").load()

print("\n\n")
#df = spark.read.format("mongo").option("uri", f"mongodb://{db_addr}/TravelReviews.reviews").load()

#client = MongoClient('mongodb://localhost:27017')
#db = client.TravelReviews
#collection = db.reviews


epsilon = .5
p = .1



data_len = (data.count())
basket_len = floor(data_len * p)
basket_epsilon = epsilon * p

print((basket_len))
print(basket_epsilon)

import random
indexes = list(range(data_len))
random.shuffle(indexes)

indexes = ([indexes[i*basket_len:(i+1)*basket_len] for i in range(floor(len(indexes)/basket_len))])
print(indexes[0])
print(list(spark.find({"_id": {"$in" : indexes[0]}})))



'''