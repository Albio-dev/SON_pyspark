import benchmark

# Set the variables to use for the benchmark. None for automatic

# The different percentages of the dataset to use
data_sizes = [.01, .05, .1, .15, .2, .25, .3, .5, .8, 1]
# How many partitions to force
partitions = [None]
# The support to use
supports = [.1]


# Run the gridsearch with the given lists of parameters
benchmark.gridsearch(data_sizes=data_sizes, partitions=partitions, supports = supports)