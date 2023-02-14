import benchmark

data_sizes = [.1,.5,1]
partitions = [4, 8]
supports = [.5]

partition_sizes = [None]
samples_per_partition = [None]

benchmark.gridsearch(data_sizes=data_sizes, partitions=partitions, supports = supports, partition_sizes=partition_sizes, samples_per_partition=samples_per_partition)