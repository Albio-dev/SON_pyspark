from pymongo import MongoClient
import csv

client = MongoClient('mongodb://localhost:27017')
db = client.database_name
collection = db.collection_name

file = 'path/to/file'

# Open the file and read it
with open(file, 'r') as f:
    csv_reader = csv.DictReader(f, delimiter=';')

    for line in csv_reader:
        # Extract data from line
        field1 = line['field1']
        field2 = line['field2']
        field3 = line['field3']
        # ...

        # Create a document
        document = {'field1': field1, 'field2': field2, 'field3': field3}

        # Insert the document in the collection
        collection.insert_one(document)

# Close the connection
client.close()
