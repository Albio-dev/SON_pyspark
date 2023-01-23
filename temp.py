fi = [['beaches', 'resorts', 'religious', 'bars', 'parks'], [('beaches', 'religious'), ('beaches', 'parks'), ('beaches', 'resorts'), ('bars', 'parks'), ('parks', 'resorts'), ('religious', 'resorts'), ('religious', 'parks'), ('beaches', 'bars')], [('religious', 'parks', 'resorts'), ('beaches', 'bars', 'parks'), ('beaches', 'parks', 'resorts'), ('beaches', 'religious', 'parks'), ('beaches', 'religious', 'resorts')], [('beaches', 'religious', 'parks', 'resorts')]]


frequent_itemsets = fi[0]
new_frequent_itemsets= fi[1]        

print(f'frequent_itemset this round: {frequent_itemsets}\n\nnew_frequent_itemsets : {new_frequent_itemsets}\n')
frequent_itemsets = new_frequent_itemsets + \
    [(i, ) if isinstance(i, str) else i \
        for i in frequent_itemsets \
            if all( \
                [not set([i]).issubset(k) if isinstance(i, str) else not set(i).issubset(k) for k in new_frequent_itemsets] \
                )]
print(f'Resulting frequent itemsets: {frequent_itemsets}')