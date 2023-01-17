from pymongo import MongoClient
from math import floor

client = MongoClient('mongodb://localhost:27017')
db = client.TravelReviews
collection = db.reviews


epsilon = .5
p = .1

data_len = (collection.count_documents({}))
basket_len = floor(data_len * p)
basket_epsilon = epsilon * p

print((basket_len))
print(basket_epsilon)

import random
indexes = list(range(data_len))
random.shuffle(indexes)

indexes = ([indexes[i*basket_len:(i+1)*basket_len] for i in range(floor(len(indexes)/basket_len))])
print(indexes[0])
list(collection.find({"_id":1}, {"$in" : indexes[0][0]}))


    