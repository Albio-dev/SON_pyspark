from pymongo import MongoClient
import csv

client = MongoClient('mongodb://localhost:27017')
db = client.OnlineRetail
collection = db.transactions

client.admin.command('enableSharding', db.name)
client.admin.command('shardCollection', db.name + '.' + collection.name, key={'_id': 1})

file = './Datasets/Online Retail/Online Retail.csv'

# Open the file and read it
with open(file, 'r') as f:
    csv_reader = csv.DictReader(f, delimiter=',')
    
    items = []
    invoice_no = ''
    for line in csv_reader:
        if line['InvoiceNo'].startswith('C'):
            continue
        if line['InvoiceNo'] != invoice_no:
            if len(items) > 0:
                document = {"_id": invoice_no, "items": items}
                collection.insert_one(document)
            items = []
        invoice_no = line['InvoiceNo']
        stock_code = line['StockCode']
        items.append(stock_code)
        
    # Insert the last document
    document = {"_id": invoice_no, "items": items}
    collection.insert_one(document)

# Close the connection
client.close()