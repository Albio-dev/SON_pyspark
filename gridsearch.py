import benchmark

data_sizes = [.1, .5, 1]
partitions = [1, 2, None]
supports = [.2, .4, .6, .8]

benchmark.gridsearch(data_sizes=data_sizes, partitions=partitions, supports = supports)