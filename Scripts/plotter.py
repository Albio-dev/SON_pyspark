import matplotlib.pyplot as plt

def plot(ax, dataset, title):

    frequencies = [x[1] for x in dataset]
    x_indexes = list(range(0,len(dataset)))
    x_labels = [x[0] for x in dataset]
    ax.set_xticks(x_indexes, x_labels)
    ax.plot(frequencies)
    ax.set_title(title)
    ax.set_xlabel('Frequent itemsets')
    ax.set_ylabel('Frequency')