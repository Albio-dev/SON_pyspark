basket = [['parks', 'beaches', 'religious'], ['parks', 'beaches', 'religious'], ['parks', 'beaches', 'religious'], ['parks', 'beaches', 'religious'], ['parks', 'beaches', 'religious'], ['parks', 'beaches', 'religious'], ['parks', 'beaches', 'religious'], ['bars', 'resorts', 'parks', 'beaches'], ['resorts', 'parks', 'beaches', 'religious'], ['parks', 'beaches', 'religious'], ['parks', 'beaches', 'religious'], ['resorts', 'parks', 'beaches'], ['parks', 'beaches', 'religious'], ['parks', 'beaches', 'religious'], ['parks', 'beaches', 'religious'], ['bars', 'parks', 'beaches'], ['parks', 'beaches', 'religious'], ['parks', 'beaches', 'religious'], ['parks', 'beaches', 'religious'], ['parks', 'beaches', 'religious'], ['parks', 'beaches', 'religious'], ['bars', 'resorts', 'parks', 'beaches'], ['parks', 'beaches', 'religious'], ['parks', 'beaches', 'religious'], ['parks', 'beaches'], ['parks', 'beaches', 'religious'], ['parks', 'beaches', 'religious'], ['resorts', 'parks', 'beaches', 'religious'], ['clubs', 'resorts', 'parks', 'beaches', 'religious'], ['parks', 'beaches', 'religious'], ['parks', 'beaches'], ['parks', 'beaches', 'religious'], ['bars', 'parks', 'beaches'], ['parks', 'beaches'], ['bars', 'parks', 'beaches'], 
['parks', 'beaches'], ['parks', 'beaches', 'religious'], ['parks', 'beaches', 'religious'], ['parks', 
'beaches', 'religious'], ['parks', 'beaches', 'religious'], ['parks', 'beaches'], ['parks', 'beaches', 'religious'], ['parks', 'beaches', 'religious'], ['parks', 'beaches', 'religious'], ['parks', 'beaches'], ['parks', 'beaches'], ['parks', 'beaches', 'religious'], ['parks', 'beaches', 'theaters', 'religious'], ['parks', 'beaches', 'religious'], ['parks', 'beaches', 'religious'], ['bars', 'parks', 'beaches'], ['resorts', 'parks', 'beaches', 'religious'], ['parks', 'beaches', 'religious'], ['parks', 'beaches'], ['parks', 'beaches', 'religious'], ['parks', 'beaches', 'religious'], ['parks', 'beaches', 'religious'], ['parks', 'beaches', 'religious'], ['parks', 'beaches', 'religious'], ['parks', 'beaches', 'religious'], ['parks', 'beaches', 'religious'], ['parks', 'beaches'], ['parks', 'beaches', 'religious'], ['parks', 'beaches', 'religious'], ['parks', 'beaches', 'religious'], ['parks', 'beaches', 'religious'], ['parks', 'beaches', 'religious'], ['resorts', 'parks', 'beaches', 'religious'], ['parks', 'beaches', 'religious'], ['parks', 'beaches', 'religious'], ['resorts', 'parks', 'beaches', 'religious'], ['parks', 'beaches', 'religious'], ['parks', 'beaches', 'religious'], ['parks', 'beaches', 'religious'], ['parks', 'beaches', 'religious'], ['bars', 'parks', 'beaches'], ['parks', 'beaches', 'religious'], ['parks', 'beaches', 'religious'], ['parks', 'beaches', 'religious'], ['parks', 'beaches', 'theaters', 
'religious'], ['parks', 'beaches', 'religious'], ['parks', 'beaches', 'religious'], ['parks', 'beaches', 'religious'], ['museums', 'resorts', 'parks', 'beaches', 'religious'], ['parks', 'beaches', 'religious'], ['parks', 'beaches', 'religious'], ['resorts', 'parks', 'beaches', 'religious'], ['parks', 'beaches', 'religious'], ['parks', 'beaches', 'religious'], ['parks', 'beaches', 'religious'], ['parks', 'beaches', 'religious'], ['parks', 'beaches'], ['resorts', 'parks', 'beaches', 'religious'], ['parks', 
'beaches', 'religious'], ['parks', 'beaches', 'religious'], ['resorts', 'parks', 'beaches', 'religious'], ['parks', 'beaches', 'religious'], ['parks', 'religious']]

def count_frequencies(data_chunk):
    itemsets = data_chunk[0]
    data = data_chunk[1]
    values = []        
    #print(f'In itemset {itemsets}')
    for i in itemsets:
        #print(f'Item: {i}')
        count = 0        
        for j in data:
            #print(f'basket: {j}')
            if type(i) not in [list, tuple, set]:
                if i in j:
                    count += 1
            else:
                if all(items in j for items in i):
                    count += 1
        values.append((i, count))
    #print(f'{values}\n\n\n')
    return values

e = .95
p = 10

def apriori2(basket):

    basket_size = len(basket)
    basket_e = e/p

    items = []
    frequent_itemsets = []

    items = list(set([k for j in basket for k in j]))    
    temp = count_frequencies([items, basket])

    frequent_items = [i[0] for i in temp if i[1]/basket_size >= basket_e]
    print(frequent_items)

    new_frequent_itemsets = frequent_items
    while new_frequent_itemsets != []:
        frequent_itemsets+=new_frequent_itemsets

        new_candidate_itemsets = [(j, ) + (k, ) if isinstance(j, str) else j + (k,) for j in new_frequent_itemsets for k in frequent_items if k not in j]
        new_candidate_itemsets = [tuple(j) for j in {frozenset(i) for i in new_candidate_itemsets}]
        
        temp = count_frequencies([new_candidate_itemsets, basket])

        new_frequent_itemsets = [i[0] for i in temp if i[1]/basket_size >= basket_e]

        print(new_frequent_itemsets)

    print(frequent_itemsets)


    


apriori2(basket)
