#5Adding a New Column:
from pyspark.sql import SparkSession

# Initialize SparkSession
spark = SparkSession.builder \
    .appName("DataFrameExample") \
    .getOrCreate()

# Create a DataFrame from a list of tuples
data = [("Alice", 34), ("Bob", 45), ("Charlie", 28)]
df = spark.createDataFrame(data, ["Name", "Age"])



# Adding a new column
df_with_new_column = df.withColumn("NewAge", df["Age"] + 10)

# Show DataFrame with new column
df_with_new_column.show()

#Grouping and Aggregation:

from pyspark.sql.functions import avg

# Grouping and aggregation
agg_df = df.groupBy("Name").agg(avg("Age"))

# Show aggregated DataFrame
agg_df.show()

#Joining DataFrames:

# Creating another DataFrame
data2 = [("Alice", "Sales"), ("Bob", "Marketing"), ("Charlie", "HR")]
df2 = spark.createDataFrame(data2, ["Name", "Department"])

# Joining DataFrames
joined_df = df.join(df2, "Name", "inner")

# Show joined DataFrame
joined_df.show()