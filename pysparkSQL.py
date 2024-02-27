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


#Register DataFrame as a Table:


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

#Using SQL Functions:


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

#Joining Tables using SQL:


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

