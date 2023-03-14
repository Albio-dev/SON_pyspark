from pyspark.sql import SparkSession
import logging

import lib.SON

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
# port: the port of the mongodb database
def loadspark(selectedDataset = 0, forcePartitions = None, logger = None, db_addr = '127.0.0.1', port = '27017'):
    datasets = {0: 'TravelReviews.reviews', 1:'OnlineRetail.transactions', 'benchmark': 'BenchmarkData.data'}

    if logger is not None:
        logger.info(f'Run with dataset {datasets[selectedDataset]}')

    # Create spark session. Get mongo connector and configure spark
    spark = (SparkSession.builder
        .master("local[*]")
        .config('spark.executor.memory', '8g')
        .config('spark.driver.memory', '8g')
        .config('spark.jars.packages', 'org.mongodb.spark:mongo-spark-connector:10.0.2')
        .config("spark.mongodb.read.connection.uri", f"mongodb://{db_addr}:{port}/{datasets[selectedDataset]}")
        .config("spark.mongodb.write.connection.uri", f"mongodb://{db_addr}:{port}/{datasets[selectedDataset]}")
        .getOrCreate()
        )
    input_data = spark.read.format("mongodb").load()

    if logger is not None:
        logger.debug(f'Connecting to {spark.conf.get("spark.mongodb.read.connection.uri")}')
        logger.debug(f'Core on this worker: {spark.sparkContext.defaultParallelism}')
        logger.info(f'Dataset size: {input_data.count()}')
        logger.info(f'Automatically created {input_data.rdd.getNumPartitions()} partitions')

    # Force partitions if required
    if forcePartitions is not None:
        if logger is not None:
            logger.debug(f'Forced {forcePartitions} partitions')
        input_data = input_data.repartition(forcePartitions)
        if logger is not None:
            logger.info(f'Partitions after forcing: {input_data.rdd.getNumPartitions()}')
    else:
        input_data = input_data.repartition(spark.sparkContext.defaultParallelism)
    # Extract the RDD from the dataframe
    data = input_data.rdd.mapPartitions(lambda x: [j.items for j in x])
    
    return data


# SON execution function given the parameters
# data: the data to use SON onto. A pyspark RDD is required
# epsilon: the support required for an itemset to be considered supported
# logger: the logger object to use to logging
def execute_SON(data, epsilon = .85, logger = None):
    if logger is not None:
        logger.info(f'Support set to {epsilon}')

    # SON algorithm class creation
    son = lib.SON.SON(data, epsilon)

    # SON algorithm execution
    frequent_itemsets = son.candidate_frequent_itemsets()

    # Check if the dataset is too small to produce frequent itemsets with the given support
    # Writes on SON.log
    if frequent_itemsets is None:
        if logger is not None:
            logger.error('Dataset or support too small. Cannot produce frequent itemsets')
        return data.context.emptyRDD()
    
    return frequent_itemsets

# Code to be executed when this script is called directly with a sample SON execution
if __name__ == '__main__':
    # Create the logger object
    logger = loadlogger()
    # Create the spark context and load data
    data = loadspark(selectedDataset=0, logger = logger, port = '27017')

    # Execute algorithm
    print(execute_SON(data, 0.1))
