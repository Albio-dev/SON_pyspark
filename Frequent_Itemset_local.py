from pyspark import SparkContext, SparkConf
import logging
import sys
import lib.SON
import lib.preprocessing

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
def loadspark(selectedDataset = 0, forcePartitions = None, logger = None, benchmarkData = None):
    if selectedDataset == 'benchmark' and benchmarkData is None:
        print('No benchmark data provided')
        sys.exit(1)
    datasets = {0: lib.preprocessing.tripadvisor_review, 1:lib.preprocessing.online_retail, 'benchmark': benchmarkData}

    if selectedDataset == 'benchmark':
        if logger is not None:
            logger.info(f'Run with dataset {selectedDataset}')

        config = (SparkConf()
                  .setAppName('SON')
                  .setMaster('local')
                  .set('spark.executor.memory', '4g')
                  .set('spark.driver.memory', '4g')
                  )
        spark = SparkContext(conf=config)
        data = spark.parallelize(benchmarkData)
        if forcePartitions is not None:
            if logger is not None:
                logger.debug(f'Forced {forcePartitions} partitions')
            data = data.repartition(forcePartitions)
            if logger is not None:
                logger.info(f'Partitions after forcing: {data.getNumPartitions()}')
        return data

    if logger is not None:
        logger.info(f'Run with dataset {datasets[selectedDataset]}')

    spark = SparkContext.getOrCreate()
    
    data = spark.parallelize(datasets[selectedDataset]())

    if forcePartitions is not None:
        if logger is not None:
            logger.debug(f'Forced {forcePartitions} partitions')
        data = data.repartition(forcePartitions)
        if logger is not None:
            logger.info(f'Partitions after forcing: {data.getNumPartitions()}')

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
    return frequent_itemsets

# Code to be executed when this script is called directly with a sample SON execution
if __name__ == '__main__':
    # Create the logger object
    logger = loadlogger()
    # Create the spark context and load data
    data = loadspark(logger = logger)

    # Execute algorithm
    print(execute_SON(data, 0.1))
