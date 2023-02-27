from lib.utils import count_frequencies

# Apriori algorithm. Requires data as list and accepted support
def apriori(data, total_support, total_data_size):
    # Get actual batch size
    basket_size = len(data)
    # Scale support for a single batch
    support = basket_size / total_data_size * total_support
    # print(f'basket_size: {basket_size} - total_data_size: {total_data_size} - total_support: {total_support} - support: {support}')

    frequent_itemsets = []
    # Extract item list
    items = list(set([k for j in data for k in j]))
    # Get items frequencies
    temp = count_frequencies(items, data)
    # Filter only frequent ones and put singlets in tuples
    frequent_items = [i[0] for i in temp if i[1] / basket_size >= support]

    new_frequent_itemsets = frequent_items
    # While there are frequent itemsets
    while new_frequent_itemsets != []:
        # Store the itemsets
        frequent_itemsets += new_frequent_itemsets
        # Form new candidates appending singlets to the last frequent itemsets
        new_candidate_itemsets = [(j, ) + (k, ) if isinstance(j, str) else j + (k,) for j in new_frequent_itemsets for k in frequent_items if k not in j]
        # Filter out repeated elements (not considering order)
        new_candidate_itemsets = [tuple(j) for j in {frozenset(i) for i in new_candidate_itemsets}]        
        # Count occurrences of the new itemset
        temp = count_frequencies(new_candidate_itemsets, data)
        # Filter out non-frequent itemsets
        new_frequent_itemsets = [i[0] for i in temp if i[1] / basket_size >= support]
        
    return frequent_itemsets
