#4Filtering Rows:
from pyspark.sql import SparkSession

# Initialize SparkSession
spark = SparkSession.builder \
    .appName("DataFrameExample") \
    .getOrCreate()

# Create a DataFrame from a list of tuples
data = [("Alice", 34), ("Bob", 45), ("Charlie", 28)]
df = spark.createDataFrame(data, ["Name", "Age"])

# Filtering rows
filtered_df = df.filter(df["Age"] > 30)

# Show filtered DataFrame
filtered_df.show()
