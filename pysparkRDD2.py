from pyspark import SparkContext, SparkConf

# Initialize SparkConf and SparkContext
conf = SparkConf().setAppName("RDDOperationsExample")
sc = SparkContext(conf=conf)

# Create an RDD from a list of tuples
data = [
    ("John", 25),
    ("Alice", 30),
    ("Bob", 35),
    ("Alice", 40),
    ("John", 45),
    ("Eva", 28),
    ("David", 33),
    ("Emily", 29),
    ("James", 37),
    ("Sophia", 31),
    ("Michael", 42),
    ("Emma", 27),
    ("Olivia", 34),
    ("William", 39),
    ("Isabella", 36)
]
rdd = sc.parallelize(data)

# Transformation: Group RDD by key
grouped_rdd = rdd.groupByKey()

# Transformation: Sort RDD by key
sorted_rdd = rdd.sortByKey()

# Action: Collect sorted RDD to the driver
sorted_data = sorted_rdd.collect()

# Print results
print("Grouped RDD:")
for key, values in grouped_rdd.collect():
    print(key, list(values))
    from pyspark import SparkContext, SparkConf


# Stop SparkContext
sc.stop()

