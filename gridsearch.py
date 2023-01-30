import benchmark

data_sizes = [.5, 1]
partitions = [1, 2, None]
supports = [.5, .9]

partition_sizes = [None, 1, 10]
samples_per_partition = [None, 20, 50]

benchmark.gridsearch(data_sizes=data_sizes, partitions=partitions, supports = supports, partition_sizes=partition_sizes, samples_per_partition=samples_per_partition)