# Frequent_itemset_spark

How to run an instance of SON:
With local data (already present in Datasets folder) pyspark is necessary. Install with
pip install pyspark
careful that additional steps may be required (e.g. installing spark on the system).
Also install matplotlib for plotting the results.

then simply run 
python Frequent_Itemset_local.py



For a mongodb execution, the following steps are required:
1. Install mongodb on the system
2. Install pymongo with pip install pymongo
3. Load the data into the database with the scripts present in the folder dataset_importers
4. Change the connection number in the script Frequent_Itemset_db.py accordingly. 0 for travel reviews, 1 for online retail (line 89)
5. Run the script Frequent_Itemset_mongodb.py with python Frequent_Itemset_db.py

Execution informations can be found in the file logs/SON.log



How to run an benchmark:
1. Install pyspark with pip install pyspark
2. install pymongo with pip install pymongo
3. execute the script benchmark.py with python benchmark.py

The benchmark program takes care of loading the data where it needs to.
Results are saved in the file logs/benchmark.log

The benchmark dataset can be changed by defining a preprocessing function which returns the dataset as list in the Scripts/preprocessing.py file and then passing it as argument to the benchmark preprocessing function call (line 113).