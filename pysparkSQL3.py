#3 Using SQL Functions:


from pyspark.sql import SparkSession
from pyspark.sql.functions import col

# Initialize SparkSession
spark = SparkSession.builder \
    .appName("SQLFunctionsExample") \
    .getOrCreate()

# Create DataFrame
data = [("Alice", 34), ("Bob", 45), ("Charlie", 28)]
df = spark.createDataFrame(data, ["Name", "Age"])

# Register DataFrame as a table
df.createOrReplaceTempView("people")

# Execute SQL query using SQL functions
result = spark.sql("SELECT Name, Age, Age * 2 AS DoubleAge FROM people WHERE Age > 30")

# Show result
result.show()

# Stop SparkSession
spark.stop()