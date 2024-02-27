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


#Creating a DataFrame:


from pyspark.sql import SparkSession

# Initialize SparkSession
spark = SparkSession.builder \
    .appName("DataFrameExample") \
    .getOrCreate()

# Create a DataFrame from a list of tuples
data = [("Alice", 34), ("Bob", 45), ("Charlie", 28)]
df = spark.createDataFrame(data, ["Name", "Age"])

# Show DataFrame
df.show()



#Selecting Columns:


# Selecting columns
selected_df = df.select("Name", "Age")

# Show selected DataFrame
selected_df.show()

#Filtering Rows:


# Filtering rows
filtered_df = df.filter(df["Age"] > 30)

# Show filtered DataFrame
filtered_df.show()

#Adding a New Column:

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