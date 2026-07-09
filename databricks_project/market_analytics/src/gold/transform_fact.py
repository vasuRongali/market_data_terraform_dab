import argparse
from pyspark.sql import SparkSession


def load_fact_daily_pricing(spark: SparkSession):
    spark.sql("""
        INSERT INTO gold.market.fact_daily_pricing
        SELECT
            dateDim.date_id,
            stateDim.state_id,
            marketDim.market_id,
            productDim.product_id,
            varietyDim.variety_id,
            silverFact.row_id,
            silverFact.arrival_in_tonnes,
            silverFact.maximum_price,
            silverFact.minimum_price,
            silverFact.modal_price,
            current_timestamp() AS created_at,
            current_timestamp() AS updated_at
        FROM silver.market.daily_pricing_silver silverFact
        LEFT JOIN gold.market.dim_date dateDim
            ON date_format(silverFact.date_of_pricing, 'dd/MM/yyyy') = dateDim.calendar_date
        LEFT JOIN gold.market.dim_state stateDim
            ON silverFact.state_name = stateDim.state_name
        LEFT JOIN gold.market.dim_market marketDim
            ON silverFact.market_name = marketDim.market_name
        LEFT JOIN gold.market.dim_product productDim
            ON silverFact.product_name = productDim.product_name
           AND silverFact.productgroup_name = productDim.productgroup_name
        LEFT JOIN gold.market.dim_variety varietyDim
            ON silverFact.variety = varietyDim.variety
    """)


def log_watermark(spark: SparkSession, status: str):
    spark.sql(f"""
        INSERT INTO bronze.processrunlogs.DELTALAKEHOUSE_PROCESS_RUNS
            (PROCESS_NAME, PROCESSED_FILE_TABLE_DATE, PROCESS_STATUS)
        VALUES
            ('gold_fact_daily_pricing_load', current_date(), '{status}')
    """)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--catalog", default="gold")
    args = parser.parse_args()

    spark = SparkSession.builder.getOrCreate()
    spark.sql(f"USE CATALOG {args.catalog}")

    try:
        load_fact_daily_pricing(spark)
        log_watermark(spark, "Completed")
        print("Gold fact table loaded successfully")
    except Exception as e:
        log_watermark(spark, "Failed")
        raise e


if __name__ == "__main__":
    main()