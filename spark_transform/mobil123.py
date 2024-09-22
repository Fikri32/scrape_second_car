from pyspark.sql import SparkSession
from pyspark.sql.functions import regexp_replace, col, trim
from pyspark.sql.types import IntegerType
import pandas as pd
import pendulum
import os
import time

now = pendulum.now().format("YYYY-MM-DD")
spark = SparkSession.builder.appName("Second Car").getOrCreate()


input_path = f"/home/miracle/mobil/scrape/output/mobil123/{now}/"

if not os.path.exists(input_path):
    raise FileNotFoundError(f"Directory {input_path} does not exist")

csv_files = [f for f in os.listdir(input_path) if f.endswith(".csv")]
if not csv_files:
    raise FileNotFoundError(f"no CSV files found in {input_path}")

print(f"CSV files found : {csv_files}")

df_mobil123 = spark.read.csv(
    input_path + "*.csv", header=True, inferSchema=True, sep=","
)
df_mobil123 = df_mobil123.withColumn("Price", col("Price").cast(IntegerType()))
df_mobil123 = df_mobil123.withColumn("KM", col("KM").cast(IntegerType()))
df_mobil123 = df_mobil123.withColumn("Seats", col("Seats").cast(IntegerType()))


# Convert Spark DataFrame to Pandas DataFrame
pandas_df = df_mobil123.toPandas()

# Define the output file name and path
output_file_name = f"all_second_car_data_{now}.csv"
output_path = os.path.join(input_path, output_file_name)

# Write the Pandas DataFrame to a single CSV file
pandas_df.to_csv(output_path, index=False)

print(f"Cleaned data has been saved to {output_path}")
