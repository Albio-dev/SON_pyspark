from pymongo import MongoClient

from itertools import combinations

client = MongoClient('mongodb://localhost:27017')
db = client.TravelReviews
collection = db.reviews

def apriori(scores, support = 100):
    # First pass

    # Translate items to numbers
    # Create a dictionary with the items and their corresponding number
    items = {}
    index = 0
    # List of counts for each item
    # The i-th element of the list is the count of the i-th item
    counts = []
    for s in scores:
        for i in s:
            if i not in items:
                items[i] = index
                index += 1
                counts.append(0)
            counts[items[i]] += 1

    # Print the frequent items
    # for i in items:
    #     if counts[items[i]] >= support:
    #         print(i, counts[items[i]])

    # Remove the infrequent items
    items = {i: items[i] for i in items if counts[items[i]] >= support}
    counts = [counts[i] for i in items.values()]

    # Second pass

    # Create a list of all possible pairs of items
    # TODO: is there a better way to do this (without creating all the possible pairs)?
    pairs = list(combinations(items.keys(), 2))
    # print(pairs)

    # List of counts for each pair
    # The i-th element of the list is the count of the i-th pair
    counts_pairs = [0] * len(pairs)

    # Count the number of times each pair appears
    for s in scores:
        for p in pairs:
            if p[0] in s and p[1] in s:
                counts_pairs[pairs.index(p)] += 1

    # Print the frequent pairs
    for p in pairs:
        if counts_pairs[pairs.index(p)] >= support:
            print(p, counts_pairs[pairs.index(p)])


# Retrieve all the documents
documents = collection.find({})
# Get the scores of each user
scores = []
for d in documents:
    scores.append(d['good_scores'])

# apriori([
#     ['A', 'B', 'C'],
#     ['B', 'C', 'D'],
#     ['A', 'C', 'D'],
#     ['A', 'B', 'C', 'D'],
#     ['B', 'D']
# ])
apriori(scores)
