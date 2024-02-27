#2Register DataFrame as a Table:


from pyspark.sql import SparkSession

# Initialize SparkSession
spark = SparkSession.builder \
    .appName("SQLExample") \
    .getOrCreate()

# Create DataFrame
data = [("Alice", 34), ("Bob", 45), ("Charlie", 28)]
df = spark.createDataFrame(data, ["Name", "Age"])

# Register DataFrame as a table
df.createOrReplaceTempView("people")

# Execute SQL query
result = spark.sql("SELECT * FROM people WHERE Age > 30")

# Show result
result.show()

# Stop SparkSession
spark.stop()