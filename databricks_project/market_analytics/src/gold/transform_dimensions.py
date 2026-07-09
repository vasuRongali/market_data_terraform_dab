import argparse
from pyspark.sql import SparkSession



def load_single_column_dim(spark, source_col, target_table, id_col,target_col):
    spark.sql(f"""
        INSERT INTO {target_table}
        ({id_col}, {target_col}, created_at, updated_at)
        SELECT
            src.{source_col},
            ROW_NUMBER() OVER (ORDER BY src.{source_col})
              + COALESCE((SELECT MAX({id_col}) FROM {target_table}), 0) AS {id_col},
            current_timestamp() AS CREATED_AT,
            current_timestamp() AS UPDATED_AT
        FROM (
            SELECT DISTINCT {source_col}
            FROM silver.market.daily_pricing_silver
            WHERE {source_col} IS NOT NULL
        ) src
        LEFT JOIN {target_table} tgt
            ON src.{source_col} = tgt.{source_col}
        WHERE tgt.{source_col} IS NULL
    """)


def load_product_dim(spark):
    spark.sql("""
        INSERT INTO gold.market.dim_product
              
        SELECT
            src.PRODUCTGROUP_NAME,
            src.PRODUCT_NAME,
            ROW_NUMBER() OVER (ORDER BY src.PRODUCTGROUP_NAME, src.PRODUCT_NAME)
              + COALESCE((SELECT MAX(PRODUCT_ID) FROM gold.market.dim_product), 0) AS PRODUCT_ID,
            current_timestamp() AS CREATED_AT,
            current_timestamp() AS UPDATED_AT
        FROM (
            SELECT DISTINCT PRODUCTGROUP_NAME, PRODUCT_NAME
            FROM silver.market.daily_pricing_silver
            WHERE PRODUCT_NAME IS NOT NULL
        ) src
        LEFT JOIN gold.market.dim_product tgt
            ON src.PRODUCT_NAME = tgt.PRODUCT_NAME
           AND src.PRODUCTGROUP_NAME = tgt.PRODUCTGROUP_NAME
        WHERE tgt.PRODUCT_ID IS NULL
    """)


def log_watermark(spark):
    spark.sql("""
        INSERT INTO bronze.processrunlogs.DELTALAKEHOUSE_PROCESS_RUNS
            (PROCESS_NAME, PROCESSED_FILE_TABLE_DATE, PROCESS_STATUS)
        VALUES
            ('gold_dimension_load', current_date(), 'Completed')
    """)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--catalog", default="gold")
    args = parser.parse_args()

    spark = SparkSession.builder.getOrCreate()


    load_single_column_dim(
        spark,
        source_col="STATE_NAME",
        target_table="gold.market.dim_state",
        id_col="STATE_ID",
        target_col="STATE_NAME"
    )

    load_single_column_dim(
        spark,
        source_col="MARKET_NAME",
        target_table="gold.market.dim_market",
        id_col="MARKET_ID",
        target_col="MARKET_NAME"
    )
    load_single_column_dim(
        spark,
        source_col="VARIETY",
        target_table="gold.market.dim_variety",
        id_col="VARIETY_ID",
        target_col="VARIETY"
    )

    

    load_product_dim(spark)

    log_watermark(spark)

    print("Gold dimension tables loaded successfully")


if __name__ == "__main__":
    main()