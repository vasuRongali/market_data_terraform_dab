import argparse
from pyspark.sql import SparkSession


def transform_daily_pricing_silver(spark: SparkSession):
    """
    Incremental load from bronze.daily_pricing into silver.daily_pricing_silver.
    Uses watermark table to only process new records since last successful run.
    """
    insert_sql = """
        INSERT INTO silver.market.daily_pricing_silver
        SELECT
            to_date(DATE_OF_PRICING, 'MM/dd/yyyy')     AS DATE_OF_PRICING,
            CAST(ROW_ID AS BIGINT)                      AS ROW_ID,
            STATE_NAME,
            MARKET_NAME,
            PRODUCTGROUP_NAME,
            PRODUCT_NAME,
            VARIETY,
            ORIGIN,
            CAST(ARRIVAL_IN_TONNES AS DECIMAL(18,2))    AS ARRIVAL_IN_TONNES,
            CAST(MINIMUM_PRICE     AS DECIMAL(36,2))    AS MINIMUM_PRICE,
            CAST(MAXIMUM_PRICE     AS DECIMAL(36,2))    AS MAXIMUM_PRICE,
            CAST(MODAL_PRICE       AS DECIMAL(36,2))    AS MODAL_PRICE,
            source_file_load_date,
            current_timestamp()                         AS CREATED_AT,
            current_timestamp()                         AS UPDATED_AT
        FROM bronze.market.daily_pricing_bronze
        WHERE source_file_load_date > (
            SELECT NVL(MAX(PROCESSED_FILE_TABLE_DATE), '1900-01-01')
            FROM bronze.processrunlogs.DELTALAKEHOUSE_PROCESS_RUNS
            WHERE PROCESS_NAME   = 'daily_pricing_silver'
            AND   PROCESS_STATUS = 'Completed'
        )
    """
    spark.sql(insert_sql)
    print("Silver insert completed.")


def log_watermark(spark: SparkSession):
    """Log the max source_file_load_date of the current silver batch as Completed."""
    watermark_sql = """
        INSERT INTO bronze.processrunlogs.DELTALAKEHOUSE_PROCESS_RUNS
            (PROCESS_NAME, PROCESSED_FILE_TABLE_DATE
, PROCESS_STATUS)
        SELECT
            'daily_pricing_silver',
            MAX(source_file_load_date),
            'Completed'
        FROM silver.market.daily_pricing_silver
    """
    spark.sql(watermark_sql)
    print("Watermark logged.")


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--catalog", default="bronze")
    args = parser.parse_args()

    spark = SparkSession.builder.getOrCreate()
    spark.sql(f"USE CATALOG {args.catalog}")

    try:
        transform_daily_pricing_silver(spark)
        log_watermark(spark)
    except Exception as e:
        spark.sql("""
            INSERT INTO bronze.processrunlogs.DELTALAKEHOUSE_PROCESS_RUNS
                (PROCESS_NAME, PROCESSED_FILE_TABLE_DATE, PROCESS_STATUS)
            VALUES ('daily_pricing_silver', current_timestamp(), 'Failed')
        """)
        raise e


if __name__ == "__main__":
    main()