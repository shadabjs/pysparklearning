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

