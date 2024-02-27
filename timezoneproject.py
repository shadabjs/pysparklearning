from pyspark.sql import SparkSession
from pyspark.sql.functions import col

# Initialize SparkSession
spark = SparkSession.builder \
    .appName("TimezoneAnalysis") \
    .getOrCreate()

# Load CSV data into DataFrame
csv_file_path = "timezone.csv"
df = spark.read.csv(csv_file_path, header=True, inferSchema=True)

# Show DataFrame schema and preview
df.printSchema()
df.show()

# Perform analysis or manipulation based on columns value, label, and group
# Example 1: Group by 'group' column and count records for each group
group_counts = df.groupBy("group").count()
group_counts.show()

# Example 2: Filter data where 'label' is equal to a specific value
filtered_df = df.filter(df["label"] == "specific_label")
filtered_df.show()

# Example 3: Aggregate values based on 'label' column (assuming 'value' is numeric)
label_aggregation = df.groupBy("label").agg({"value": "sum"})
label_aggregation.show()

# Exanple 4: Filter data where 'value' is greater than a specific threshold
filtered_df = df.filter(df["value"] > 100)
filtered_df.show()

# Example 5: Filter data where 'label' is not equal to a specific value
filtered_df = df.filter(df["label"] != "specific_label")
filtered_df.show()

# Example 6: Selecting only 'value' and 'label' columns
selected_df = df.select("value", "label")
selected_df.show()

# Example 7: Selecting columns and adding a new column
new_df = df.select("value", "label", (col("value") * 2).alias("value_doubled"))
new_df.show()

# Example 8: Sort data by 'value' column in ascending order
sorted_df = df.orderBy("value")
sorted_df.show()

# Example 9: Sort data by 'value' column in descending order
sorted_df_desc = df.orderBy(df["value"].desc())
sorted_df_desc.show()

# Example 10: Aggregate values based on both 'label' and 'group' columns
label_group_aggregation = df.groupBy("label", "group").agg({"value": "sum"})
label_group_aggregation.show()










# Stop SparkSession
spark.stop()
