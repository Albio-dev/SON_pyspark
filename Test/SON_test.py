from pymongo import MongoClient
import random
import sys
sys.path.append("..")
import SON

test_size = 10
p = .5
s = .5

client = MongoClient('mongodb://localhost:27017')
db = client.TestingDB
collection = db.test

collection.drop()

# LOADING TEST DATA #
for i in range( test_size):
    values =  ['A', 'B', 'C', 'D', 'E', 'F']
    test_data = {"_id": i, "scores": random.sample(values, random.randint(1, len(values)))}

    # Insert the document in the collection
    collection.insert_one(test_data)

assert collection.count_documents({}) == test_size, f"Data input size wrong. Expected database size {test_size}, got {collection.count_documents({})}"

algo = SON.SON(collection, p, s)

# Batch extraction test
assert len(algo.get_data(0)) == test_size*p, f'Retrieved data sample 0 size mismatch. Expected {test_size*p}, got {len(algo.get_data(0))}'
assert len(algo.get_data(1)) == test_size*p, f'Retrieved data sample 1 size mismatch. Expected {test_size*p}, got {len(algo.get_data(1))}'
try:
    algo.get_data(2)
    assert False, f'Managed to extract non-existant data'
except:
    pass



collection.drop()

# Close the connection
client.close()
