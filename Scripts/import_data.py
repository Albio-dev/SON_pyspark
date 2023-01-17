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
        # Create a document
        document = {list(line.values())[0].split(' ')[1]: [i + str(round(float(j))) for i, j in list(line.items())[1:]]}
        # print(document)
        
        # Insert the document in the collection
        collection.insert_one(document)

# Close the connection
client.close()
