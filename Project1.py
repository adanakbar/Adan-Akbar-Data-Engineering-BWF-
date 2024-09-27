from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, to_date, regexp_extract, lower, hour

spark = SparkSession.builder.appName("Order Data Processing").getOrCreate()

file_path = "C:\\Users\\adana\\OneDrive\\Pictures\\Desktop\\data.csv"
df = spark.read.csv(file_path, header=True, inferSchema=True)

# Step 3: Clean the order_date column
df = df.withColumn("order_date", to_date(col("order_date")))
df = df.withColumn("hour", hour(col("order_date")))

df = df.filter(~((col("hour") >= 0) & (col("hour") <= 5)))

df = df.drop("hour")

df = df.withColumn(
    "time_of_day",
    when((col("order_date").hour >= 5) & (col("order_date").hour < 12), "morning")
    .when((col("order_date").hour >= 12) & (col("order_date").hour < 18), "afternoon")
    .otherwise("evening"),
)


df = df.filter(~lower(col("product")).contains("tv"))

df = df.withColumn("product", lower(col("product")))
df = df.withColumn("category", lower(col("category")))

# Step 6: Extract the state from the purchase address
df = df.withColumn(
    "purchase_state",
    regexp_extract(col("puchase_addres"), r",\s*([A-Z]{2})\s*\d{5}$", 1),
)

final_columns = [
    "order_date",
    "time_of_day",
    "order_id",
    "product",
    "product_id",
    "category",
    "puchase_addres",
    "purchase_state",
    "quantity_order",
    "price_each",
    "cost_price",
    "turnover",
    "margin",
]

final_df = df.select(final_columns)

output_path = "processed_orders.parquet"
final_df.write.mode("overwrite").parquet(output_path)

spark.stop()
