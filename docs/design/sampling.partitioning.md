# 2. Sampling
> Step of selecting a small subset of the data to determine how to partition the data on the future step
* identify key valuse that divide the data into approximately equal-sized partitions

## Methods
generated by ChatGPT 4o
1. Systematic Sampling:
    - Selects every nth record from the dataset.
    - Useful when data is evenly distributed and stored in sequential order.
2. Stratified Sampling:
    - Divides the dataset into strata (categories) and then samples within each stratum.
    - Ensures representation of all parts of the dataset, even if the data is skewed.
3. Reservoir Sampling:
    - A specialized technique for sampling when the dataset size is unknown or the data is streamed.
    - Maintains a fixed-size sample while processing the data sequentially.

> Choice: Systematic Sampling

## Pseudocode
```
# workerSample
# input: 
#   data: set of (key, value) pairs
#   p: percentage of data to sample
#   workerCount: number of workers
# output:
#   samples: set of keys

def workerSample(data, p, workerCount)
    samples <- []
    for i <- 0 to p * len(data)
        # interval = (key.max - key.min) / (p * len(data))
        samples.append(data[data.key.min() + i * (data.keys.max() - data.keys.min()) / (p * len(data))].key)
    return samples
```

```
# sendSamples
# input
#   samples: set of samples

def sendSamples(samples)
    # send samples to workers
    send samples using grpc
```

# 3. Partitioning
> Step of dividing the data into smaller, manageable subsets for processing
* divide the data into partitions based on the key values identified in the sampling step
* determining optimal partition boundaries on Sampling step is crucial 

## Methods
generated by ChatGPT 4o
1. Range-Based Partitioning:
    - Data is divided based on sorted ranges of key values.
    - Suitable for numeric or ordered datasets.
    - Ensures that the final sorted order is easy to merge, as partitions are already sorted by range.
2. Hash-Based Partitioning:
    - A hash function is applied to each key, and the output determines the partition.
    - Useful when the dataset is not inherently ordered (e.g., text data).
    - Does not guarantee globally sorted order unless additional steps are performed.

> Choice: Range-Based Paritioning

## Pseudocode
```
# masterPickBoundaries
# input
#   keys: merged set of sampled keys, unsorted
#   workerCount: number of workers
# output
#   boundaries: set of boundary points

def masterPickBoundaries(keys, workerCount)
    # sort the keys
    keys.sort()
    # pick n-1 boundaries
    boundaries <- []
    for i <- 1 to workerCount
        boundaries.append(keys[i * len(keys) // workerCount])
    return boundaries
```

```
# sendBoundaries
# input
#   boundaries: set of boundary points

def sendBoundaries(boundaries)
    # send boundaries to workers
    send boundaries using grpc
```

```
# workerPartition
# input
#   data: set of (key, value) pairs
#   boundaries: set of boundary points
# output
#   subsets: partitioned sets of (key, value) pairs

def workerPartition(data, boundaries)
    i = 0
    subset = []
    subsets = []
    for d in data
        if d.key >= boundaries[i] and d.key < boundaries[i+1]
            subset.append(d)
        else
            subsets.append(subset)
            subset = []
    return subsets
```