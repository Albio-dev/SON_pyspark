import random

file = './Datasets/Travel Reviews/tripadvisor_review.csv'

# Append new lines to the file
user_id = 981
with open(file, 'a') as f:
    for _ in range(3000000):
        scores = [round(random.uniform(0, 4), 2) for _ in range(10)]
        line = ','.join([str(i) for i in scores])
        f.write('User ' + str(user_id) + ',' + line + '\n')
        user_id += 1
        