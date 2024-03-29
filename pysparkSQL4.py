#4Joining Tables using SQL:


from pyspark.sql import SparkSession

# Initialize SparkSession
spark = SparkSession.builder \
    .appName("SQLJoinExample") \
    .getOrCreate()

# Create DataFrames
data1 = [("Alice", 34), ("Bob", 45), ("Charlie", 28)]
df1 = spark.createDataFrame(data1, ["Name", "Age"])

data2 = [("Alice", "Sales"), ("Bob", "Marketing"), ("Charlie", "HR")]
df2 = spark.createDataFrame(data2, ["Name", "Department"])

# Register DataFrames as tables
df1.createOrReplaceTempView("people")
df2.createOrReplaceTempView("departments")

# Execute SQL query to join tables
result = spark.sql("SELECT p.Name, p.Age, d.Department FROM people p INNER JOIN departments d ON p.Name = d.Name")

# Show result
result.show()

# Stop SparkSession
spark.stop()
