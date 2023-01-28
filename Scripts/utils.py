# Utility function to count occurrences of items in the first list inside elements of the second list
# e.g.: ( [itemsets], [baskets] )
def count_frequencies(data_chunk):
    # Itemsets to check
    itemsets = data_chunk[0]
    # Batch to count in itemsets in
    data = data_chunk[1]
    values = []
    # For every itemset
    for i in itemsets:
        count = 0        
        # Check if it exists in every basket
        for j in data:
            # This check is to handle strings
            if type(i) not in [list, tuple, set]:
                if i in j:
                    count += 1
            else:
                if all(items in j for items in i):
                    count += 1
        values.append((i, count))
    return values

