from pyspark.sql import SparkSession
import logging

import Scripts.SON

# Logging functions definition
def loadlogger():

    logger = logging.getLogger('son')

    if not logger.hasHandlers():
        logger.setLevel(logging.DEBUG)
        file_handler = logging.FileHandler("logs/SON.log")
        formatter = logging.Formatter('%(asctime)s:%(levelname)s:%(message)s')
        file_handler.setFormatter(formatter)
        logger.addHandler(file_handler)

    # Silence log4j, giving trouble
    logging.getLogger('pyspark').setLevel(logging.ERROR)
    logging.getLogger("py4j").setLevel(logging.ERROR)

    return logger

# Start spark session and load dataset from mongodb with the specified parameters
# selectedDataset: which path to load from in mongo
# forcePartitions: How many partitions force onto the dataset
# logger: the logger object to use for logging 
# db_addr: the address of the mongodb database
def loadspark(selectedDataset = 0, forcePartitions = 2, logger = None, db_addr = '127.0.0.1', port = '27017', partition_size = None, samples_per_partition = None):
    datasets = {0: 'TravelReviews.reviews', 1:'OnlineRetail.transactions', 'benchmark': 'BenchmarkData.data'}
    
    #from Scripts import import_travel_reviews
    #from Scripts import import_online_retail

    if logger is not None:
        logger.info(f'Run with dataset {datasets[selectedDataset]}')
        

    spark = (SparkSession.builder
        # .appName("SON")
        # .master("local[4]")
        .config('spark.jars.packages', 'org.mongodb.spark:mongo-spark-connector:10.0.2')
        .config("spark.mongodb.read.connection.uri", f"mongodb://{db_addr}:{port}/{datasets[selectedDataset]}")
        .config("spark.mongodb.write.connection.uri", f"mongodb://{db_addr}:{port}/{datasets[selectedDataset]}")
        .getOrCreate()
        )
    if partition_size is not None:
        spark.conf.set("partitioner.options.partition.size", partition_size) # The size (in MB) for each partition.
        logger.info(f'Partition size: {partition_size}')
    if samples_per_partition is not None:
        spark.conf.set("partitioner.options.samples.per.partition", samples_per_partition) # The number of samples to take per partition.
        logger.info(f'Samples per partition: {samples_per_partition}')
    input_data = spark.read.format("mongodb").load()

    if logger is not None:
        logger.debug(f'Connecting to {spark.conf.get("spark.mongodb.read.connection.uri")}')
        logger.debug(f'Core on this worker: {spark.sparkContext.defaultParallelism}')
        logger.info(f'Dataset size: {input_data.count()}')
        logger.info(f'Automatically created {input_data.rdd.getNumPartitions()} partitions')

    if forcePartitions is not None:
        if logger is not None:
            logger.debug(f'Forced {forcePartitions} partitions')
        input_data = input_data.repartition(forcePartitions)
        if logger is not None:
            logger.info(f'Partitions after forcing: {input_data.rdd.getNumPartitions()}')

    data = input_data.rdd.mapPartitions(lambda x: [j.items for j in x])
    return data


# Preprocessing: trasformare gli item in numeri
#items = {i: items[i] for i in items if counts[items[i]] >= support}
'''
items = {}
    index = 0
    # List of counts for each item
    # The i-th element of the list is the count of the i-th item
    counts = []
    for s in scores:
        for i in s:
            if i not in items:
                items[i] = index
                index += 1
                counts.append(0)
            counts[items[i]] += 1
'''

# SON execution function given the parameters
# data: the data to use SON onto. A pyspark RDD is required
# epsilon: the support required for an itemset to be considered supported
# logger: the logger object to use to logging
def execute_SON(data, epsilon = .85, logger = None):
    if logger is not None:
        logger.info(f'Support set to {epsilon}')

    # SON algorithm class creation
    son = Scripts.SON.SON(data, epsilon)

    # SON algorithm execution
    frequent_itemsets = son.candidate_frequent_itemsets()
    return frequent_itemsets

# Code to be executed when this script is called directly with a sample SON execution
if __name__ == '__main__':
    # Create the logger object
    logger = loadlogger()
    # Create the spark context and load data
    data = loadspark(logger = logger, port = '27017', forcePartitions=None)

    # Execute algorithm
    execute_SON(data, 0.3)
