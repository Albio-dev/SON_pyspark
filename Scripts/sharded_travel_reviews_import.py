from pymongo import MongoClient
import csv

client = MongoClient('mongodb://localhost:27017')
# Connect to the mongos instance on port 60000 (created by the docker-compose file)
# client = MongoClient('mongodb://localhost:60000')
db = client.TravelReviews
collection = db.reviews

client.admin.command('enableSharding', db.name)
client.admin.command('shardCollection', db.name + '.' + collection.name, key={'_id': "hashed"})

file = './Datasets/Travel Reviews/tripadvisor_review.csv'

# Open the file and read it
with open(file, 'r') as f:
    documents = []
    batch_size = 100000 # Number of documents to insert at a time
    batch_count = 0 # Number of batches inserted so far
    csv_reader = csv.DictReader(f, delimiter=',')

    for line in csv_reader:
        # Create a document with the following structure:
        # {"_id": n, "good_scores": ["A", "B", "C", ...]}
        # With n identifying the user and A, B, C, ... the good scores that the user gave
        # We arbitrarily consider that a score of 2.5 or more is a good score
        good_score_limit = 2.5
        document = {"_id": int(list(line.values())[0].split(' ')[1]),
                    "items": [i for i, j in list(line.items())[1:] if float(j) >= good_score_limit]}
        
        documents.append(document)
        batch_count += 1

        # Insert the documents in batches (much faster than one at a time)
        if batch_count == batch_size:
            collection.insert_many(documents)
            batch_count = 0
            documents = []


# Close the connection
client.close()
