import benchmark

data_sizes = [.01,.05,.1, .15, .2, .25, .3]
partitions = [8]
supports = [.9]

partition_sizes = [None]
samples_per_partition = [None]

benchmark.gridsearch(data_sizes=data_sizes, partitions=partitions, supports = supports, partition_sizes=partition_sizes, samples_per_partition=samples_per_partition)