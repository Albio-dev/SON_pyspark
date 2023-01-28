from pyspark.sql import SparkSession
import logging

from Scripts import SON


datasets = {0: 'TravelReviews.reviews', 1:'OnlineRetail.transactions'}
selectedDataset = 0
db_addr = '127.0.0.1'
forcePartitions = 2


logger = logging.getLogger('son')
logger.setLevel(logging.DEBUG)
file_handler = logging.FileHandler("SON.log")
formatter = logging.Formatter('%(asctime)s:%(levelname)s:%(message)s')
file_handler.setFormatter(formatter)
logger.addHandler(file_handler)

logging.getLogger('pyspark').setLevel(logging.ERROR)
logging.getLogger("py4j").setLevel(logging.ERROR)



#from Scripts import import_travel_reviews
#from Scripts import import_online_retail



logger.info(f'Run with dataset {datasets[selectedDataset]}')

spark = (SparkSession.builder
    .config('spark.jars.packages', 'org.mongodb.spark:mongo-spark-connector_2.12:3.0.1')
    .config("spark.mongodb.input.uri", f"mongodb://{db_addr}/{datasets[selectedDataset]}")
    .config("spark.mongodb.output.uri", f"mongodb://{db_addr}/{datasets[selectedDataset]}")
    # .config("spark.mongodb.input.partitioner", "MongoPaginateByCountPartitioner")
    .config("spark.mongodb.input.partitionerOptions.partitionKey", "_id")
    # .config("spark.mongodb.input.partitionerOptions.numberOfPartitions", "4")
    .getOrCreate()
    )

input_data = spark.read.format("mongo").load()

logger.debug(f'Connecting to {spark.conf.get("spark.mongodb.input.uri")}')
logger.debug(f'Core on this worker: {spark.sparkContext.defaultParallelism}')
logger.info(f'Dataset size: {input_data.count()}')
logger.info(f'Automatically created {input_data.rdd.getNumPartitions()} partitions')

if forcePartitions is not None:
    logger.debug(f'Forced {forcePartitions} partitions')
    input_data = input_data.repartition(forcePartitions)
    logger.info(f'Partitions after forcing: {input_data.rdd.getNumPartitions()}')

data = input_data.rdd.mapPartitions(lambda x: [j.items for j in x])


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

epsilon = .85
logger.info(f'Support set to {epsilon}')

son = SON.SON(data, epsilon)

frequent_itemsets = son.candidate_frequent_itemsets()