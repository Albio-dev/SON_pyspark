#from pymongo import MongoClient
from math import floor

class SON:

    def __init__(self, database, p, s):
        self.db = database
        self.p = p

        total_len = database.count_documents({})

        self.basket_size = floor(total_len * p)
        self.basket_s = s * p

    def get_data(self, i):

        interval = list(range(i*self.basket_size, \
            (i+1)*self.basket_size))

        return list(self.db.find({"_id": {"$in" : interval}}))

    def candidate_fi(self):

        # Split batches
        # Apriori on batch
        # Emit fi


    