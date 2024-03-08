from pyspark.sql import SparkSession
from pyspark.sql.types import *

spark = SparkSession.builder.appName("DataFrameCreation").getOrCreate()

# Sample Data (Note: StringType is generally good enough for dates)
schema = StructType([
    StructField("id", IntegerType()), 
    StructField("start_date", TimestampType()), 
    StructField("end_date", TimestampType()),
    StructField("data", StringType())
])

data = [
    (6696902, "2024-02-02 10:30:00", "2024-03-02 18:17:00", "xfbxcvbx"),
    (8535098, "2024-02-02 09:30:00", "2024-03-02 18:17:00", "xfbxcvbx"),
    (8858051, "2024-02-02 19:30:00", "2024-02-02 22:30:00", "xfbxcvbx") 
]

# Create the DataFrame
df = spark.createDataFrame(data)
df1=df.withColumnRenamed("_1","id").withColumnRenamed("_2","start_date").withColumnRenamed("_3","end_date").withColumnRenamed("_4","data")

# Display the DataFrame
df1.show()


from pyspark.sql.functions import explode, col, expr

# First, let's convert the start_date and end_date columns to timestamps
df2 = df1.withColumn("start_date", col("start_date").cast("timestamp")) \
         .withColumn("end_date", col("end_date").cast("timestamp"))

# Define the range of dates for each row
df3 = df2.withColumn("date_range", expr("sequence(start_date, end_date, interval 1 hour)"))

# Explode the date ranges into separate rows
df4 = df3.select("id", explode("date_range").alias("hourly_date"), "data")

# Display the DataFrame with completed hourly data
df4.show(truncate=False)


