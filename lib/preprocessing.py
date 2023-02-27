import csv

def online_retail():
    file = './Datasets/Online Retail/Online Retail.csv'
    dataset = []
    with open(file, 'r') as f:
        csv_reader = csv.DictReader(f, delimiter=',')
        
        items = []
        invoice_no = ''
        for line in csv_reader:
            if line['InvoiceNo'].startswith('C'):
                continue
            if line['InvoiceNo'] != invoice_no:
                if len(items) > 0:
                    document = items
                    dataset.append(document)
                items = []
            invoice_no = line['InvoiceNo']
            stock_code = line['StockCode']
            items.append(stock_code)
            
        # Insert the last document
        document = items
        dataset.append(document)
    return dataset


# Function to load and preprocess data
def tripadvisor_review():
    file = './Datasets/Travel Reviews/tripadvisor_review.csv'
    dataset = []
    # Open the file and read it
    with open(file, 'r') as f:
        csv_reader = csv.DictReader(f, delimiter=',')

        for line in csv_reader:
            # We arbitrarily consider that a score of 2.5 or more is a good score
            good_score_limit = 2.5
            dataset.append([i for i, j in list(line.items())[1:] if float(j) >= good_score_limit])

    return dataset
