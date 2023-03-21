import matplotlib.pyplot as plt
import numpy as np

support = .3
partitions = 2
dataset = 'easy'

with open("./logs/benchmark.log", "r") as f:
    log = f.readlines()

benchmarks = ["Apriori execution time", "DB SON execution time", "Local SON execution time", "DB FI execution time"]
def extract_data(filter):
    return [float(i.split(":")[4].strip()[:5]) for i in log if filter in i]

def autolabel(rects):
    """
    Attach a text label above each bar displaying its height
    """
    for rect in rects:
        height = rect.get_height()
        plt.text(rect.get_x() + rect.get_width()/2., 1.05*height,
                '%d' % int(height),
                ha='center', va='bottom')
        
tr_sizes = [i.split(":")[3].split(" ")[3] for i in log if "Loaded dataset" in i]
tr_partitions = [i.split(":")[-1].strip() for i in log if "partitions" in i]

x_data = tr_sizes

apriori_time = extract_data(benchmarks[0])
SON_DB_time = extract_data(benchmarks[1])
SON_LOCAL_TIME = extract_data(benchmarks[2])
FI_DB_time = extract_data(benchmarks[3])

ind = np.arange(len(x_data))
width=.2
i = -2
plt.figure(figsize=(9, 7))
a = plt.bar(ind+(i*width), apriori_time, width, label='Apriori', color='r')
i += 1
b = plt.bar(ind+(i*width), SON_DB_time, width, label='SON DB', color='g')
i += 1
c = plt.bar(ind+(i*width), SON_LOCAL_TIME, width, label='SON Local', color='b')
i += 1
d = plt.bar(ind+(i*width), FI_DB_time, width, label='FI DB', color='y')
plt.legend()

plt.xticks(ind, x_data, rotation=-15)
plt.xlabel('Dataset size')
plt.ylabel('Time (s)')
plt.title(f'Execution time for different algorithms, {dataset} dataset\nSupport: {support}, Partitions: {tr_partitions[0]}')
autolabel(a)
autolabel(b)
autolabel(c)
autolabel(d)
plt.show()