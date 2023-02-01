from Scripts import apriori
import csv
import time
import Frequent_Itemset
from pymongo import MongoClient
import logging
from pyspark import SparkContext
from pyspark.sql import SparkSession

benchmark_logger = logging.getLogger('benchmark')
benchmark_logger.setLevel(logging.INFO)
file_handler = logging.FileHandler("logs/benchmark.log")
formatter = logging.Formatter('%(asctime)s:%(message)s')
file_handler.setFormatter(formatter)
benchmark_logger.addHandler(file_handler)

# Data loading function. Loads data from file, extracts a specified subset
# and loads it into mongo at BenchmarkData.data
def load_data(preprocessing_function, perc_ds = 1, ip = 'localhost', port = 27017):
    client = MongoClient(f'mongodb://{ip}:{port}')
    db = client.BenchmarkData
    collection = db.data
    
    # Load dataset
    dataset = preprocessing_function()

    # This is the place to resize it
    size = int((len(dataset)*perc_ds)//1)
    test_dataset = dataset[:size]

    # Loading on db
    collection.drop()
    
    # client.admin.command('enableSharding', db.name)
    # client.admin.command('shardCollection', db.name + '.' + collection.name, key={'_id': "hashed"})

    collection.insert_many([{'items': i} for i in test_dataset])
    client.close()

    benchmark_logger.info(f'Loaded dataset. Using {size} samples')
    return test_dataset

# Executes apriori and then SON with the specified parameters
# support: the support accepted in both apriori and SON
# partitions: how many partitions use in SON
# logging: if SON should use the logging functionalities
def benchmark(dataset, support = 0.5, partitions = None, logging = True, partition_size = None, samples_per_partition = None ):
    benchmark_logger.info(f'Benchmark with support: {support}, partitions: {partitions}, partition_size: {partition_size}, samples_per_partition: {samples_per_partition}')

    # Run and time apriori
    start_time = time.time()
    apriori_result = apriori.apriori(dataset, support)
    benchmark_logger.info(f'Apriori execution time: {time.time() - start_time}s')

    # Use logging if so specified
    if logging:
        logger = Frequent_Itemset.loadlogger()
    else:
        logger = None
    # Load spark session with specified parameters
    # selectedDataset: dataset to use
    # forcePartitions: how many partitions to use. None for automatic
    benchmark_logger.info(f'Started loading DB data...')
    data = Frequent_Itemset.loadspark(selectedDataset='benchmark', forcePartitions=partitions, logger=logger, partition_size=partition_size, samples_per_partition=samples_per_partition)
    benchmark_logger.info(f'Data loaded.')
    # Run and time SON
    start_time = time.time()
    SON_result = Frequent_Itemset.execute_SON(data, support, logger)
    benchmark_logger.info(f'DB SON execution time: {time.time() - start_time}s')

    
    #spark = SparkContext(appName='benchmark')
    spark = data.context
    benchmark_logger.info(f'Started loading LOCAL data...')
    data = spark.parallelize(dataset, partitions)
    benchmark_logger.info(f'Data loaded.')
    start_time = time.time()
    SON_result = Frequent_Itemset.execute_SON(data, support, logger)
    benchmark_logger.info(f'Local SON execution time: {time.time() - start_time}s')

    # Automatic frequent itemsets
    # DB
    ss = SparkSession.getActiveSession()
    input_data = ss.read.format("mongodb").load()
    start_time = time.time()
    auto_result = input_data.freqItems(('items',), support=support)
    benchmark_logger.info(f'DB FI execution time: {time.time() - start_time}s')
    

    
    '''
    # Check whether results are equal
    if apriori_result == SON_result:
        benchmark_logger.info(f'Functions results were equal')
    else:
        benchmark_logger.info(f'Functions results were not equal: apriori: {apriori_result}, SON: {SON_result}')
    '''



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
    client = MongoClient('mongodb://localhost:27017')
    db = client.TravelReviews
    collection = db.reviews

    # Load dataset
    dataset = []
    for document in collection.find():
        dataset.append(document['items'])

    return dataset
    # file = './Datasets/Travel Reviews/tripadvisor_review.csv'
    # dataset = []
    # # Open the file and read it
    # with open(file, 'r') as f:
    #     csv_reader = csv.DictReader(f, delimiter=',')

    #     for line in csv_reader:
    #         # Create a document with the following structure:
    #         # {"_id": n, "good_scores": ["A", "B", "C", ...]}
    #         # With n identifying the user and A, B, C, ... the good scores that the user gave
    #         # We arbitrarily consider that a score of 2.5 or more is a good score
    #         good_score_limit = 2.5
    #         dataset.append([i for i, j in list(line.items())[1:] if float(j) >= good_score_limit])

    # return dataset


# Code to execute when the file is executed directly
if __name__ == '__main__':
    # Example benchmark with half the dataset, automatic partitioning and support 0.5
    # data = load_data(online_retail, perc_ds = .5, ip = 'localhost', port = 60000)
    data = load_data(tripadvisor_review, perc_ds = .5, ip = 'localhost')
    benchmark(data, support = .5)


def gridsearch(data_sizes, partitions, supports, partition_sizes, samples_per_partition):

    # Iterate over every required data percentage
    for i in data_sizes:
        # data = load_data(online_retail, perc_ds = i, port = '60000')
        data = load_data(tripadvisor_review, perc_ds = i)
        # Iterate over partitions and supports
        
        
        for j in partitions:
            for n in partition_sizes:
                for m in samples_per_partition:
                    for k in supports:
                        benchmark(data, partitions = j, support=k, partition_size=n, samples_per_partition=m)