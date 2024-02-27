#RDD example

#example 1


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

# Transformation: Filter RDD based on a condition
filtered_rdd = rdd.filter(lambda x: x[1] > 30)

# Transformation: Map RDD to create key-value pairs
key_value_rdd = rdd.map(lambda x: (x[0], 1))

# Action: Count the number of elements
count = rdd.count()

# Action: Collect RDD to the driver
collected_data = rdd.collect()

# Action: Reduce by key to find the total age of each person
total_age_by_person = rdd.reduceByKey(lambda x, y: x + y)

# Print results
print("Filtered RDD:")
print(filtered_rdd.collect())
print("\nKey-Value RDD:")
print(key_value_rdd.collect())
print("\nTotal count:", count)
print("\nCollected Data:")
print(collected_data)
print("\nTotal Age by Person:")
print(total_age_by_person.collect())

# Stop SparkContext
sc.stop()



#example 2

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
    
print("\nSorted RDD:")
for key, value in sorted_data:
    print(key, value)

# Stop SparkContext
sc.stop()




#dataframe example

#example 1

from pyspark.sql import SparkSession

# Initialize SparkSession
spark = SparkSession.builder \
    .appName("DataFrameExample") \
    .getOrCreate()

# Create DataFrame
df = spark.createDataFrame([(1, 'Alice'), (2, 'Bob')], ['id', 'name'])

# Show DataFrame
df.show()

# Filter DataFrame
filtered_df = df.filter(df['id'] == 1)
filtered_df.show()

# Stop SparkSession
spark.stop()



#pyspark SQL

#example 1

from pyspark.sql import SparkSession

# Initialize SparkSession
spark = SparkSession.builder \
    .appName("SQLExample") \
    .getOrCreate()

# Create DataFrame
df = spark.createDataFrame([(1, 'Alice'), (2, 'Bob')], ['id', 'name'])

# Register DataFrame as a table
df.createOrReplaceTempView("people")

# Execute SQL query
result = spark.sql("SELECT * FROM people WHERE id = 2")

# Show result
result.show()

# Stop SparkSession
spark.stop()





