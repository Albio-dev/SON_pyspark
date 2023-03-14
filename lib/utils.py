# Utility function to count occurrences of items in the first list inside elements of the second list
# e.g.: ( [itemsets], [baskets] )
def count_frequencies(itemsets, data):
    values = {}

    for i in data:
        for j in itemsets:
            if type(j) not in [list, tuple, set]:
                if j in i:
                    values[j] = values.get(j, 0) + 1
            else:
                if set(j) in i:
                    values[j] = values.get(j, 0) + 1
    return list(values.items())
