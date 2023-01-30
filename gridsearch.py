import benchmark

data_sizes = [.5, 1]
partitions = [1, 2, None]
supports = [.5, .9]

benchmark.gridsearch(data_sizes=data_sizes, partitions=partitions, supports = supports)