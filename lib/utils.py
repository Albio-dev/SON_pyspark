# Utility function to count occurrences of items in the first list inside elements of the second list
# e.g.: ( [itemsets], [baskets] )
def count_frequencies(itemsets, data):
    values = {}

    for i in data:
        for j in itemsets:
            if type(j) == str:
                j = set((j,))
            
            if type(i) == set:
                if set(j).issubset(i):
                    values[tuple(j)] = values.get(tuple(j), 0) + 1
            else:
                if all(k in i for k in j):
                    values[tuple(j)] = values.get(tuple(j), 0) + 1
    return list(values.items())
