from lib import apriori
import time
import Frequent_Itemset_db
import Frequent_Itemset_local
from pymongo import MongoClient
import logging
from pyspark.sql import SparkSession
import lib.preprocessing
import matplotlib.pyplot as plt
from lib.utils import count_frequencies
import lib.plotter as plotter

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
    
    collection.insert_many([{'items': i} for i in test_dataset])
    client.close()

    benchmark_logger.info(f'Loaded dataset. Using {size} samples')
    return test_dataset


# Executes apriori and then SON with the specified parameters
# support: the support accepted in both apriori and SON
# partitions: how many partitions use in SON
# logging: if SON should use the logging functionalities
def benchmark(dataset, support = 0.5, partitions = None, logging = True, partition_size = None, samples_per_partition = None, plot = True):
    benchmark_logger.info(f'Benchmark with support: {support}, partitions: {partitions}, partition_size: {partition_size}, samples_per_partition: {samples_per_partition}')

    # Run and time apriori
    start_time = time.time()
    benchmark_logger.info(f'Starting Apriori execution')
    apriori_result = apriori.apriori(dataset, support, len(dataset))
    benchmark_logger.info(f'Apriori result: {apriori_result}')
    benchmark_logger.info(f'Apriori execution time: {time.time() - start_time}s')

    # Use logging if so specified
    if logging:
        logger = Frequent_Itemset_db.loadlogger()
    else:
        logger = None
    
    # SON DB
    # Load spark session with specified parameters
    # selectedDataset: dataset to use
    # forcePartitions: how many partitions to use. None for automatic
    benchmark_logger.info(f'Starting DB execution...')
    data = Frequent_Itemset_db.loadspark(selectedDataset='benchmark', forcePartitions=partitions, logger=logger, partition_size=partition_size, samples_per_partition=samples_per_partition).cache()
    # Run and time DB SON
    start_time = time.time()
    SON_db_result = Frequent_Itemset_db.execute_SON(data, support, logger).collect()
    benchmark_logger.info(f'SON result: {SON_db_result}')
    benchmark_logger.info(f'DB SON execution time: {time.time() - start_time}s')

    # Automatic frequent itemsets DB
    # Get previously loaded spark session
    ss = SparkSession.getActiveSession()
    input_data = ss.read.format("mongodb").load()
    benchmark_logger.info(f'Starting method execution.')
    # Run and time freqItems
    start_time = time.time()
    auto_result = input_data.freqItems(('items',), support=support).collect()
    benchmark_logger.info(f'Auto result: {auto_result}')
    benchmark_logger.info(f'DB FI execution time: {time.time() - start_time}s')
    
    # Close spark session
    data.context.stop()

    # SON local
    # Create new spark context
    benchmark_logger.info(f'Starting LOCAL execution...')
    data = Frequent_Itemset_local.loadspark(selectedDataset='benchmark', forcePartitions=partitions, logger=logger, partition_size=partition_size, samples_per_partition=samples_per_partition, benchmarkData=dataset)
    # Run and time local SON
    start_time = time.time()
    SON_local_result = Frequent_Itemset_local.execute_SON(data, support, logger).collect()
    benchmark_logger.info(f'SON result: {SON_local_result}')
    benchmark_logger.info(f'Local SON execution time: {time.time() - start_time}s')

    # Clear spark context
    data.context.stop()

    # Plot algorithms results, if plot = True
    if plot:
        fig, axs = plt.subplots(2, 2)

        # plot apriori_result        
        plotter.plot(axs[0][0], set(count_frequencies(apriori_result, dataset)), 'Apriori')
        plotter.plot(axs[0][1], set(SON_db_result), 'DB SON')
        plotter.plot(axs[1][0], set(count_frequencies(auto_result, dataset)), 'Spark FreqItems')
        plotter.plot(axs[1][1], set(SON_local_result), 'local SON')

        plt.show()


# Code to execute when the file is executed directly
if __name__ == '__main__':
    print('Executing preprocessing...')
    data = load_data(lib.preprocessing.online_retail, perc_ds = .2, ip = 'localhost')
    print('Preprocessing done. Executing benchmark...')
    benchmark(data, support = .1, plot = True)


# Grid search function for automated benchmarking
def gridsearch(data_sizes, partitions, supports, partition_sizes, samples_per_partition):
    # Iterate over every required data percentage
    for i in data_sizes:
        data = load_data(lib.preprocessing.online_retail, perc_ds = i)

        # Iterate over partitions and supports
        for j in partitions:
            # Iterate over partition sizes and samples per partition (db connector parameters)
            for n in partition_sizes:
                for m in samples_per_partition:
                    for k in supports:
                        benchmark(data, partitions = j, support=k, partition_size=n, samples_per_partition=m, plot=False)