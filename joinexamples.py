from pyspark.sql import SparkSession, functions as func
from pyspark.sql.types import StructType, StructField, IntegerType, DateType, StringType

spark = SparkSession.builder.master("local[*]").appName("Sales_App").getOrCreate()

sales_schema = StructType(
    [
        StructField("Product_ID", IntegerType(), True),
        StructField("Customer_ID", StringType(), True),
        StructField("Order_date", DateType(), True),
        StructField("Location", StringType(), True),
        StructField("Source_order", StringType(), True),
    ]
)

menu_schema = StructType(
    [
        StructField("Product_ID", IntegerType(), True),
        StructField("Product_name", StringType(), True),
        StructField("Product_price", StringType(), True),
    ]
)

sales_df = spark.read.csv("sales.csv", inferSchema=True, schema=sales_schema)
menu_df = spark.read.csv("menu.csv", inferSchema=True, schema=menu_schema)

sales_df.show(10, truncate=False)
menu_df.show(10, truncate=False)

# Q.1.Joining DataFrames: Joining sales_df and menu_df based on the "Product_ID" column.

joined_df = sales_df.join(menu_df, "Product_ID", "inner")
joined_df.show(10, truncate=False)

# Q.2.Inner Join: This returns only the rows where there is a match in both DataFrames.

inner_join_df = sales_df.join(menu_df, "Product_ID", "inner")
inner_join_df.show(10, truncate=False)   

# Q.3.Left Outer Join: This returns all the rows from the left DataFrame, and the matched rows from the right DataFrame. If there's no match, null values are filled in for the right DataFrame columns.

left_outer_join_df = sales_df.join(menu_df, "Product_ID", "left_outer")
left_outer_join_df.show(10, truncate=False)


# Q.4.Right Outer Join: This returns all the rows from the right DataFrame, and the matched rows from the left DataFrame. If there's no match, null values are filled in for the left DataFrame columns.


right_outer_join_df = sales_df.join(menu_df, "Product_ID", "right_outer")
right_outer_join_df.show(10, truncate=False)


# Q.5.Full Outer Join: This returns all the rows from both DataFrames. If there's no match, null values are filled in for the missing columns on the respective side.


full_outer_join_df = sales_df.join(menu_df, "Product_ID", "full_outer")
full_outer_join_df.show(10, truncate=False)


# Q.6.Left Semi Join: This returns only the rows from the left DataFrame where there is a match in the right DataFrame. It doesn't include any columns from the right DataFrame.


left_semi_join_df = sales_df.join(menu_df, "Product_ID", "left_semi")
left_semi_join_df.show(10, truncate=False)


# Q.7.find the frequency of customer visited

Frequency_Of_Customer_Visited = (
    sales_df.groupBy(func.col("Customer_ID"))
    .agg(func.count(func.col("Customer_ID")).alias("Customer_Frequency"))
    .orderBy(func.desc(func.col("Customer_Frequency")))
)

Frequency_Of_Customer_Visited.show()





# Q.8.Total Amount of sales yearly

Total_Amount_Of_Sales_Yearly = (
    sales_df.withColumn("Year", func.year(func.col("Order_date")))
    .join(menu_df, sales_df["Product_ID"] == menu_df["Product_ID"])
    .groupBy(func.col("Year").alias("Order_Year"))
    .agg(func.sum(func.col("Product_price")).alias("Total_Amount_Spent"))
    .orderBy(func.asc(func.col("Order_Year")))
)

Total_Amount_Of_Sales_Yearly.show()


# Q.9.Find the total sales by order source

Total_Sales_By_Order_Source = (
    sales_df.join(menu_df, sales_df["Product_ID"] == menu_df["Product_ID"])
    .groupBy(func.col("Source_order").alias("Source"))
    .agg(func.sum(func.col("Product_price")).alias("Total_Ordered_Amount"))
    .orderBy(func.desc(func.col("Total_Ordered_Amount")))
)

Total_Sales_By_Order_Source.show()



# Q.10..find the frequency of customer visited to the Restaurant

Frequency_Of_Customer_Visited_To_Restaurant = (
    sales_df.filter(func.col("Source_order") == "Restaurant")
    .groupBy(func.col("Customer_ID"))
    .agg(func.count(func.col("Customer_ID")).alias("Customer_Frequency"))
    .orderBy(func.desc(func.col("Customer_Frequency")))
)

Frequency_Of_Customer_Visited_To_Restaurant.show()






