# Frequent_itemset_spark
This file explains the instructions to run the project.

## How to run
We implemented SON using both data on a local machine (`Frequent_Itemset_local.py`) and stored on a MongoDB database (`Frequent_Itemset_db.py`).
In both cases `pyspark` is necessary. Install with
```shell
pip install pyspark
```
Other additional steps may be required (e.g. installing spark on the system).
`matplotlib` is also used for plotting the results:
```shell
pip install matplotlib
```

If you want to run SON using data on a MongoDB database you also need to do the following steps:
1. Install MongoDB on the system (the installation depends from the OS)
2. Install `pymongo` with
```shell
pip install pymongo
```

## Run with local data
Local datasets are already present in the Datasets/ folder. To start the algorithm run
```shell
python Frequent_Itemset_local.py
```
You can change the selected dataset in the script `Frequent_Itemset_local.py`: 0 for travel reviews, 1 for online retail (line X)

Execution informations can be found in the file `logs/SON.log`.

## Run with data stored on MongoDB
For a MongoDB execution, the following steps are required:
1. Load the data into the database with the scripts present in the folder dataset_importers/:
```shell
python dataset_importers/import_travel_reviews.py
```
or
```shell
python dataset_importers/import_online_retail.py
```
2. Change the connection number in the script `Frequent_Itemset_db.py` accordingly. 0 for travel reviews, 1 for online retail (line X)
3. Run the script `Frequent_Itemset_db.py` with
```shell
python Frequent_Itemset_db.py
```

Execution informations can be found in the file `logs/SON.log`.

## How to run a benchmark:
Follow the instructions explained in the [How to run](#how-to-run) section
Then execute the script `benchmark.py` with
```shell
python benchmark.py
```

The benchmark program takes care of loading the data where it needs to.
Results are saved in the file `logs/benchmark.log`.

The benchmark dataset can be changed by defining a preprocessing function which returns the dataset as list in the `Scripts/preprocessing.py` file and then passing it as argument to the benchmark preprocessing function call (line 123).

It is possible to configure the benchmark by changing the parameters in the file at line 125.
By changing the support parameter it is possible to change the frequency threshold for the frequent itemsets, while by changing the partitions parameter it is possible to change the number of partitions used by the algorithm. By default is set to None, wich lets the specific
partitioner assign it. The local instance will by default use one partition per core, while the Database version will use the connector partitioner.
By setting partitions anything other than *None* will force both DB and local versions to use the specified number of partitions.

Note: **Choosing the number of partitions is an important operation because if we have too many, the support of each partition will be too low for the algorithm to find frequent itemsets inside the single partition (Apriori in our case) to work well. Otherwise, if the number of partitions is too low we will not appreciate the benefits of the parallelization that Spark can apply to the computation.**
