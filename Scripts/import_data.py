from pymongo import MongoClient
import csv

client = MongoClient('mongodb://localhost:27017')
db = client.TravelReviews
collection = db.reviews

file = '../Datasets/Travel Reviews/tripadvisor_review.csv'

# Open the file and read it
with open(file, 'r') as f:
    csv_reader = csv.DictReader(f, delimiter=',')

    for line in csv_reader:
        # Create a document with the following structure:
        # {"_id": n, "good_scores": ["A", "B", "C", ...]}
        # With n identifying the user and A, B, C, ... the good scores that the user gave
        # We arbitrarily consider that a score of 2.5 or more is a good score
        good_score_limit = 2.5
        document = {"_id": int(list(line.values())[0].split(' ')[1]),
                    "good_scores": [i for i, j in list(line.items())[1:] if float(j) >= good_score_limit]}
        
        # Insert the document in the collection
        collection.insert_one(document)

# Close the connection
client.close()
