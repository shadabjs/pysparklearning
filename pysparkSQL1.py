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

