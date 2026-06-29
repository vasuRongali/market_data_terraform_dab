import argparse
from datetime import datetime
import pandas as pd
from pyspark.sql import SparkSession
from pyspark.sql.functions import current_timestamp


def get_next_source_file_date(spark: SparkSession, process_name: str) -> str:
    """Read the watermark table to find the next date to ingest."""
    query = f"""
        SELECT NVL(MAX(PROCESSED_FILE_TABLE_DATE) + 1, '2023-01-01') AS NEXT_SOURCE_FILE_DATE
        FROM bronze.processrunlogs.DELTALAKEHOUSE_PROCESS_RUNS
        WHERE PROCESS_NAME = '{process_name}' AND PROCESS_STATUS = 'Completed'
    """
    result_df = spark.sql(query)
    return result_df.collect()[0]["NEXT_SOURCE_FILE_DATE"]


def build_source_url(base_url: str, folder: str, file_date: str) -> str:
    """Build the HTTP source URL for a given date (e.g. PW_MW_DR_01132023.csv)."""
    formatted_date = datetime.strptime(str(file_date), "%Y-%m-%d").strftime("%m%d%Y")
    file_name = f"PW_MW_DR_{formatted_date}.csv"
    return f"{base_url}{folder}{file_name}"


def fetch_source_data(source_url: str) -> pd.DataFrame:
    """Pull the CSV directly from the HTTP source into pandas."""
    print(f"Fetching: {source_url}")
    return pd.read_csv(source_url)


def write_to_bronze(spark: SparkSession, pandas_df: pd.DataFrame, sink_path: str):
    """Convert to Spark DataFrame and append to the bronze landing zone."""
    spark_df = spark.createDataFrame(pandas_df)
    (
        spark_df
        .withColumn("source_file_load_date", current_timestamp())
        .write
        .mode("append")
        .option("header", "true")
        .csv(sink_path)
    )
    print(f"Wrote {spark_df.count()} rows to {sink_path}")


def log_watermark(spark: SparkSession, process_name: str, file_date: str, status: str):
    """Record this run in the watermark/process-log table."""
    insert_sql = f"""
        INSERT INTO bronze.processrunlogs.DELTALAKEHOUSE_PROCESS_RUNS
        (PROCESS_NAME, PROCESSED_FILE_TABLE_DATE, PROCESS_STATUS)
        VALUES ('{process_name}', '{file_date}', '{status}')
    """
    spark.sql(insert_sql)
    print(f"Logged watermark: {process_name} -> {file_date} ({status})")


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--process_name", required=True)
    parser.add_argument("--source_base_url", required=True)
    parser.add_argument("--source_folder", required=True)
    parser.add_argument("--sink_storage_account", required=True)
    parser.add_argument("--sink_layer_name", default="bronze")
    parser.add_argument("--sink_folder_name", required=True)
    args = parser.parse_args()

    spark = SparkSession.builder.getOrCreate()

    next_file_date = get_next_source_file_date(spark, args.process_name)
    source_url = build_source_url(args.source_base_url, args.source_folder, next_file_date)
    sink_path = (
        f"abfss://{args.sink_layer_name}@{args.sink_storage_account}"
        f".dfs.core.windows.net/{args.sink_folder_name}"
    )

    try:
        pandas_df = fetch_source_data(source_url)
        write_to_bronze(spark, pandas_df, sink_path)
        log_watermark(spark, args.process_name, next_file_date, "Completed")
    except Exception as e:
        log_watermark(spark, args.process_name, next_file_date, "Failed")
        raise e


if __name__ == "__main__":
    main()
