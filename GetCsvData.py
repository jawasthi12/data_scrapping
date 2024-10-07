import requests
import pandas as pd
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, FloatType, IntegerType
from pyspark.sql.functions import regexp_replace, col, when


# Function 1: Download CSV file from URL
def download_csv(url, output_file):
    response = requests.get(url)
    with open(output_file, 'wb') as file:
        file.write(response.content)
    print(f"CSV downloaded: {output_file}")


# Function 2: Load CSV into Pandas DataFrame
def load_to_pandas(file_path):
    try:
        df = pd.read_csv(file_path, encoding='utf-8' ,low_memory=False)
    except UnicodeDecodeError:
        df = pd.read_csv(file_path, encoding='ISO-8859-1')
    print("Loaded data into Pandas DataFrame")
    return df


# Function 3: Convert Pandas DataFrame to Spark DataFrame
def convert_to_spark(pandas_df, spark, schema=None):
    if schema:
        spark_df = spark.createDataFrame(pandas_df, schema=schema)
    else:
        spark_df = spark.createDataFrame(pandas_df)
    print("Converted to Spark DataFrame")
    return spark_df


# Function 4: Data Cleaning and Manipulation
def clean_data(spark_df):
    spark_df = spark_df.fillna('')

    # Additionally, handle 'NaN' as a string and replace it with empty string as well
    spark_df = spark_df.replace('NaN', '')
    numeric_columns = ['magnitude']
    for col_name in numeric_columns:
        # Replace non-numeric characters except dots
        spark_df = spark_df.withColumn(col_name, regexp_replace(col(col_name), '[^0-9.]', ''))
        # Cast to FloatType
        spark_df = spark_df.withColumn(col_name, col(col_name).cast(IntegerType()))
        spark_df = spark_df.withColumn(col_name, col(col_name).cast(StringType()))

    # Assuming you want to prepend '$' sign to `data_value` column
    spark_df = spark_df.withColumn(
        "data_value",
        when(~col("data_value").startswith('$'),  # Check if the value doesn't start with $
             regexp_replace(col("data_value"), '^', '\\$'))  # Prepend $ to those values (escaped with \\)
        .otherwise(col("data_value"))  # Leave values with $ unchanged
    )
    print("Data cleaned")
    return spark_df


# Function 5: Define the Spark Schema
def define_schema():
    schema = StructType([
        StructField("series_reference", StringType(), True),
        StructField("period", FloatType(), True),
        StructField("data_value", FloatType(), True),
        StructField("status", StringType(), True),
        StructField("units", StringType(), True),
        StructField("magnitude", IntegerType(), True),
        StructField("subject", StringType(), True),
        StructField("group", StringType(), True),
        StructField("series_title_1", StringType(), True),
        StructField("series_title_2", StringType(), True),
        StructField("series_title_3", StringType(), True),
        StructField("series_title_4", StringType(), True),
        StructField("series_title_5", StringType(), True),
    ])
    return schema


# Function 6: Load the cleaned Spark DataFrame into PostgreSQL
def load_to_postgres(spark_df):
    jdbc_url = "jdbc:postgresql://localhost:5433/datascrapping"  # 'host.docker.internal' resolves to localhost in Docker
    jdbc_properties = {
        "user": "postgres",
        "password": "postgres",
        "driver": "org.postgresql.Driver"
    }

    # Write the DataFrame to the PostgreSQL table
    spark_df.write.jdbc(url=jdbc_url, table="cleaned_data", mode="overwrite", properties=jdbc_properties)
    print("Data loaded into PostgreSQL")


# Function 7: Main execution logic
def main():
    csv_url = "https://www.stats.govt.nz/assets/Uploads/International-trade/International-trade-June-2024-quarter/Download-data/overseas-trade-indexes-June-2024-quarter-provisional-revised.csv"
    output_file = "dataset.csv"

    # Step 1: Download CSV
    download_csv(csv_url, output_file)

    # Step 2: Load CSV into Pandas DataFrame
    pandas_df = load_to_pandas(output_file)

    # Step 3: Initialize Spark
    spark = SparkSession.builder \
        .appName("CSV to Spark") \
        .config("spark.jars", "./postgresql-42.7.3.jar") \
        .getOrCreate()

    # Step 4: Define schema for Spark DataFrame
    schema = define_schema()

    # Step 5: Convert Pandas DataFrame to Spark DataFrame with schema
    spark_df = convert_to_spark(pandas_df, spark, schema=schema)

    # Step 6: Perform data cleaning and manipulation
    cleaned_spark_df = clean_data(spark_df)

    # Step 7: Load the cleaned data into PostgreSQL
    load_to_postgres(cleaned_spark_df)
    # load_to_postgres(spark_df)


if __name__ == "__main__":
    main()
