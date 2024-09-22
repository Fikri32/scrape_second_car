from pyspark.sql import SparkSession
from pyspark.sql.functions import regexp_replace, col, trim, when
from pyspark.sql.types import LongType, IntegerType
import pandas as pd
import pendulum
import os


def transform_data():
    # Initialize Spark session
    now = pendulum.now().format("YYYY-MM-DD")
    spark = SparkSession.builder.appName("oto_car").getOrCreate()

    # Path to the CSV files
    input_path = f"/home/miracle/mobil/scrape/output/oto/{now}/"

    # Check if the directory exists and contains CSV files
    if not os.path.exists(input_path):
        raise FileNotFoundError(f"Directory {input_path} does not exist")

    csv_files = [f for f in os.listdir(input_path) if f.endswith(".csv")]
    if not csv_files:
        raise FileNotFoundError(f"No CSV files found in {input_path}")

    print(f"CSV files found: {csv_files}")

    # Read all CSV files in the folder into a single DataFrame
    df_oto = spark.read.csv(
        input_path + "*.csv", header=True, inferSchema=True, sep=","
    )

    # Clean Price column
    df_oto = df_oto.withColumn(
        "Price",
        trim(regexp_replace("Price", "Bursa oto|Juta|Rp", "")),  # Remove unznted text
    )
    df_oto = df_oto.withColumn(
        "Price",
        trim(regexp_replace("Price", "[^0-9]", "")),  # Remove non-numeric characters
    )
    # Check if Price is valid and multiply by 1 million
    df_oto = df_oto.withColumn(
        "Price",
        when(
            col("Price").rlike("^\d+$"), col("Price").cast(LongType()) * 1000000
        ).otherwise(None),
    )

    # Clean KM column
    df_oto = df_oto.withColumn("KM", regexp_replace("KM", " Km|,", ""))
    df_oto = df_oto.withColumn("KM", col("KM").cast(IntegerType()))

    # Clean Seats Column
    df_oto = df_oto.withColumn("Seats", regexp_replace("Seats", " seat", ""))
    df_oto = df_oto.withColumn("Seats", col("Seats").cast(IntegerType()))

    # Handle NaN or NULL values (Optional Step)
    df_oto = df_oto.fillna({"Transmission": "Unknown", "Seats": 0, "Engine": "Unknown"})

    # Convert Spark DataFrame to Pandas DataFrame
    pandas_df = df_oto.toPandas()

    # Define the output file name and path
    output_file_name = f"all_second_car_data_{now}.csv"
    output_path = os.path.join(input_path, output_file_name)

    # Write the Pandas DataFrame to a single CSV file
    pandas_df.to_csv(output_path, index=False)

    print(f"Cleaned data has been saved to {output_path}")


transform_data()
