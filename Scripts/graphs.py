import matplotlib.pyplot as plt
import numpy as np

log = """
2023-02-16 09:39:45,235:Loaded dataset. Using 220 samples
2023-02-16 09:39:45,252:Benchmark with support: 0.9, partitions: 8, partition_size: None, samples_per_partition: None
2023-02-16 09:39:45,252:Starting Apriori execution
2023-02-16 09:39:45,494:Apriori result: []
2023-02-16 09:39:45,495:Apriori execution time: 0.2423548698425293s
2023-02-16 09:39:45,496:Starting DB execution...
2023-02-16 09:41:02,649:SON result: []
2023-02-16 09:41:02,650:DB SON execution time: 52.60351014137268s
2023-02-16 09:41:02,701:Starting method execution.
2023-02-16 09:41:03,199:Auto result: [Row(items_freqItems=[])]
2023-02-16 09:41:03,199:DB FI execution time: 0.4966728687286377s
2023-02-16 09:41:04,990:Starting LOCAL execution...
2023-02-16 09:41:42,049:SON result: []
2023-02-16 09:41:42,050:Local SON execution time: 37.05080056190491s
2023-02-16 09:41:47,019:Loaded dataset. Using 1103 samples
2023-02-16 09:41:47,043:Benchmark with support: 0.9, partitions: 8, partition_size: None, samples_per_partition: None
2023-02-16 09:41:47,043:Starting Apriori execution
2023-02-16 09:41:50,241:Apriori result: []
2023-02-16 09:41:50,241:Apriori execution time: 3.1974446773529053s
2023-02-16 09:41:50,241:Starting DB execution...
2023-02-16 09:42:49,056:SON result: []
2023-02-16 09:42:49,056:DB SON execution time: 56.82769846916199s
2023-02-16 09:42:49,113:Starting method execution.
2023-02-16 09:42:49,422:Auto result: [Row(items_freqItems=[['21030', '22804', '84031B', '84031A', '85123A', '84029G', '21622', '22834', '21034', '15056P', '15056BL', '15056N', '20679', '22113', '22111']])]
2023-02-16 09:42:49,423:DB FI execution time: 0.30916357040405273s
2023-02-16 09:42:51,290:Starting LOCAL execution...
2023-02-16 09:43:26,204:SON result: []
2023-02-16 09:43:26,204:Local SON execution time: 34.90349197387695s
2023-02-16 09:43:29,766:Loaded dataset. Using 2206 samples
2023-02-16 09:43:29,786:Benchmark with support: 0.9, partitions: 8, partition_size: None, samples_per_partition: None
2023-02-16 09:43:29,786:Starting Apriori execution
2023-02-16 09:43:36,117:Apriori result: []
2023-02-16 09:43:36,117:Apriori execution time: 6.331060409545898s
2023-02-16 09:43:36,117:Starting DB execution...
2023-02-16 09:44:21,657:SON result: []
2023-02-16 09:44:21,658:DB SON execution time: 43.69575595855713s
2023-02-16 09:44:21,708:Starting method execution.
2023-02-16 09:44:22,032:Auto result: [Row(items_freqItems=[])]
2023-02-16 09:44:22,032:DB FI execution time: 0.32413434982299805s
2023-02-16 09:44:23,915:Starting LOCAL execution...
2023-02-16 09:44:59,319:SON result: []
2023-02-16 09:44:59,319:Local SON execution time: 35.386404514312744s
2023-02-16 09:45:02,669:Loaded dataset. Using 3309 samples
2023-02-16 09:45:02,684:Benchmark with support: 0.9, partitions: 8, partition_size: None, samples_per_partition: None
2023-02-16 09:45:02,684:Starting Apriori execution
2023-02-16 09:45:11,609:Apriori result: []
2023-02-16 09:45:11,609:Apriori execution time: 8.92509913444519s
2023-02-16 09:45:11,609:Starting DB execution...
2023-02-16 09:45:52,360:SON result: []
2023-02-16 09:45:52,360:DB SON execution time: 39.060402393341064s
2023-02-16 09:45:52,400:Starting method execution.
2023-02-16 09:45:52,720:Auto result: [Row(items_freqItems=[['22916', '22917', '22918', '22919', '22921', '22920', '84879', '21136', '22285', '22284', '22256', '22258', '21609', '21613', '22300', '22301', '22427', '21636', '22445', '22783']])]
2023-02-16 09:45:52,721:DB FI execution time: 0.3204343318939209s
2023-02-16 09:45:54,464:Starting LOCAL execution...
2023-02-16 09:46:27,924:SON result: []
2023-02-16 09:46:27,924:Local SON execution time: 33.441322565078735s
2023-02-16 09:46:31,354:Loaded dataset. Using 4412 samples
2023-02-16 09:46:31,368:Benchmark with support: 0.9, partitions: 8, partition_size: None, samples_per_partition: None
2023-02-16 09:46:31,369:Starting Apriori execution
2023-02-16 09:46:43,911:Apriori result: []
2023-02-16 09:46:43,911:Apriori execution time: 12.542602300643921s
2023-02-16 09:46:43,912:Starting DB execution...
2023-02-16 09:47:27,440:SON result: []
2023-02-16 09:47:27,440:DB SON execution time: 41.8858802318573s
2023-02-16 09:47:27,486:Starting method execution.
2023-02-16 09:47:27,795:Auto result: [Row(items_freqItems=[])]
2023-02-16 09:47:27,796:DB FI execution time: 0.31014299392700195s
2023-02-16 09:47:29,539:Starting LOCAL execution...
2023-02-16 09:48:03,008:SON result: []
2023-02-16 09:48:03,008:Local SON execution time: 33.44790005683899s
2023-02-16 09:48:06,434:Loaded dataset. Using 5516 samples
2023-02-16 09:48:06,448:Benchmark with support: 0.9, partitions: 8, partition_size: None, samples_per_partition: None
2023-02-16 09:48:06,448:Starting Apriori execution
2023-02-16 09:48:22,163:Apriori result: []
2023-02-16 09:48:22,163:Apriori execution time: 15.7155179977417s
2023-02-16 09:48:22,163:Starting DB execution...
2023-02-16 09:49:03,963:SON result: []
2023-02-16 09:49:03,963:DB SON execution time: 39.8853440284729s
2023-02-16 09:49:04,010:Starting method execution.
2023-02-16 09:49:04,480:Auto result: [Row(items_freqItems=[])]
2023-02-16 09:49:04,481:DB FI execution time: 0.47016096115112305s
2023-02-16 09:49:06,097:Starting LOCAL execution...
2023-02-16 09:49:42,777:SON result: []
2023-02-16 09:49:42,777:Local SON execution time: 36.65347480773926s
2023-02-16 09:49:48,869:Loaded dataset. Using 6619 samples
2023-02-16 09:49:48,889:Benchmark with support: 0.9, partitions: 8, partition_size: None, samples_per_partition: None
2023-02-16 09:49:48,889:Starting Apriori execution
2023-02-16 09:50:15,290:Apriori result: []
2023-02-16 09:50:15,290:Apriori execution time: 26.40035605430603s
2023-02-16 09:50:15,290:Starting DB execution...
2023-02-16 09:51:16,357:SON result: []
2023-02-16 09:51:16,357:DB SON execution time: 58.931941509246826s
2023-02-16 09:51:16,412:Starting method execution.
2023-02-16 09:51:17,068:Auto result: [Row(items_freqItems=[['22423', '22699', '22697', '22698', '22366', '21430', '22630', '22662', '22629', '22382', '15056BL', '15056P', '15056N', '20679', '21166', '21181', '21770', '22993', '22722', '23179', '23178', '22494', '21260', '21906', '84971S', '23054', '23053', '23050', '23051', '23049', '23052', '22488']])]
2023-02-16 09:51:17,068:DB FI execution time: 0.655247688293457s
2023-02-16 09:51:18,735:Starting LOCAL execution...
2023-02-16 09:52:03,877:SON result: []
2023-02-16 09:52:03,877:Local SON execution time: 45.10913968086243s
2023-02-16 09:52:08,237:Loaded dataset. Using 11032 samples
2023-02-16 09:52:08,251:Benchmark with support: 0.9, partitions: 8, partition_size: None, samples_per_partition: None
2023-02-16 09:52:08,251:Starting Apriori execution
2023-02-16 09:53:00,096:Apriori result: []
2023-02-16 09:53:00,096:Apriori execution time: 51.84426975250244s
2023-02-16 09:53:00,096:Starting DB execution...
2023-02-16 09:54:07,845:SON result: []
2023-02-16 09:54:07,845:DB SON execution time: 65.0288634300232s
2023-02-16 09:54:07,937:Starting method execution.
2023-02-16 09:54:08,966:Auto result: [Row(items_freqItems=[])]
2023-02-16 09:54:08,967:DB FI execution time: 1.0302445888519287s
2023-02-16 09:54:11,203:Starting LOCAL execution...
2023-02-16 09:55:06,833:SON result: []
2023-02-16 09:55:06,834:Local SON execution time: 55.571921586990356s
2023-02-16 09:55:11,293:Loaded dataset. Using 17651 samples
2023-02-16 09:55:11,305:Benchmark with support: 0.9, partitions: 8, partition_size: None, samples_per_partition: None
2023-02-16 09:55:11,305:Starting Apriori execution
2023-02-16 09:56:27,471:Apriori result: []
2023-02-16 09:56:27,471:Apriori execution time: 76.16618967056274s
2023-02-16 09:56:27,471:Starting DB execution...
2023-02-16 09:57:33,180:SON result: []
2023-02-16 09:57:33,180:DB SON execution time: 62.58851647377014s
2023-02-16 09:57:33,258:Starting method execution.
2023-02-16 09:57:34,540:Auto result: [Row(items_freqItems=[['22890', '82483', '22890', '22350', '22741', '22809', '22423', '22349', '72817', '72818', '84836', '82486', '22960', '20751', '23401', '21623', '23078', '23484', '10135', '23500', '23378', '23376', '22952', '22998', '21411', '21407', '22909', '23321', '22469', '21561', '85150', '21181', '21903', '21166', '23254', '23256', '22418', '85035A', '23254', '85035C', '85035B', '21908', '21174', '85152', '22662', '22112', '22835', '21485', '22111', '23355', '72741', '20986', '23389', '23388', '22382', '23207', '84051', '21624', '21115', '22487', '23152', '23153', '23542', '23132', '23135', '72760B', '85124B', '21270']])]
2023-02-16 09:57:34,541:DB FI execution time: 1.2836248874664307s
2023-02-16 09:57:36,506:Starting LOCAL execution...
2023-02-16 09:58:31,455:SON result: []
2023-02-16 09:58:31,455:Local SON execution time: 54.8480019569397s
2023-02-16 09:58:35,501:Loaded dataset. Using 22064 samples
2023-02-16 09:58:35,512:Benchmark with support: 0.9, partitions: 8, partition_size: None, samples_per_partition: None
2023-02-16 09:58:35,512:Starting Apriori execution
2023-02-16 10:00:52,120:Apriori result: []
2023-02-16 10:00:52,121:Apriori execution time: 136.60869359970093s
2023-02-16 10:00:52,121:Starting DB execution...
2023-02-16 10:02:42,729:SON result: []
2023-02-16 10:02:42,730:DB SON execution time: 105.4470465183258s
2023-02-16 10:02:42,826:Starting method execution.
2023-02-16 10:02:45,502:Auto result: [Row(items_freqItems=[])]
2023-02-16 10:02:45,502:DB FI execution time: 2.675840139389038s
2023-02-16 10:02:47,506:Starting LOCAL execution...
2023-02-16 10:04:07,574:SON result: []
2023-02-16 10:04:07,574:Local SON execution time: 79.9051616191864s
"""

benchmarks = ["Apriori execution time", "DB SON execution time", "Local SON execution time", "DB FI execution time"]
def extract_data(filter):
    return [float(i.split(":")[4].strip()[:5]) for i in log.splitlines() if filter in i]

def autolabel(rects):
    """
    Attach a text label above each bar displaying its height
    """
    for rect in rects:
        height = rect.get_height()
        plt.text(rect.get_x() + rect.get_width()/2., 1.05*height,
                '%d' % int(height),
                ha='center', va='bottom')
        
tr_sizes = [i.split(":")[3].split(" ")[3] for i in log.splitlines() if "Loaded dataset" in i]

apriori_time = extract_data(benchmarks[0])
SON_DB_time = extract_data(benchmarks[1])
SON_LOCAL_TIME = extract_data(benchmarks[2])
FI_DB_time = extract_data(benchmarks[3])

ind = np.arange(10)
width=.2
i = -2
plt.figure()
a = plt.bar(ind+(i*width), apriori_time, width, label='Apriori', color='r')
i += 1
b = plt.bar(ind+(i*width), SON_DB_time, width, label='SON DB', color='g')
i += 1
c = plt.bar(ind+(i*width), SON_LOCAL_TIME, width, label='SON Local', color='b')
i += 1
d = plt.bar(ind+(i*width), FI_DB_time, width, label='FI DB', color='y')
plt.legend()

plt.xticks(ind, tr_sizes)
plt.xlabel('Sample size')
plt.ylabel('Time (s)')
plt.title('Execution time for different algorithms, "easy" dataset')
autolabel(a)
autolabel(b)
autolabel(c)
autolabel(d)
plt.show()