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
        CREATE OR REPLACE TEMP VIEW src_product_dim AS
        SELECT DISTINCT
            PRODUCT_NAME,
            PRODUCTGROUP_NAME
        FROM silver.market.daily_pricing_silver
        WHERE PRODUCT_NAME IS NOT NULL
    """)

    spark.sql("""
        CREATE OR REPLACE TEMP VIEW src_product_dim_with_id AS
        SELECT
            ROW_NUMBER() OVER (ORDER BY src.PRODUCT_NAME)
              + COALESCE((SELECT MAX(PRODUCT_ID) FROM gold.market.dim_product), 0) AS PRODUCT_ID,
            src.PRODUCTGROUP_NAME,
            src.PRODUCT_NAME
        FROM src_product_dim src
        LEFT JOIN gold.market.dim_product tgt
            ON src.PRODUCT_NAME = tgt.PRODUCT_NAME
        WHERE tgt.PRODUCT_ID IS NULL
    """)

    spark.sql("""
        MERGE INTO gold.market.dim_product tgt
        USING (
            SELECT
                PRODUCT_ID,
                PRODUCTGROUP_NAME,
                PRODUCT_NAME
            FROM src_product_dim_with_id

            UNION ALL

            SELECT
                tgt.PRODUCT_ID,
                src.PRODUCTGROUP_NAME,
                src.PRODUCT_NAME
            FROM src_product_dim src
            INNER JOIN gold.market.dim_product tgt
                ON src.PRODUCT_NAME = tgt.PRODUCT_NAME
        ) src
        ON tgt.PRODUCT_NAME = src.PRODUCT_NAME

        WHEN MATCHED
          AND tgt.PRODUCTGROUP_NAME <> src.PRODUCTGROUP_NAME
        THEN UPDATE SET
            tgt.PRODUCTGROUP_NAME = src.PRODUCTGROUP_NAME,
            tgt.UPDATED_AT = current_timestamp()

        WHEN NOT MATCHED THEN
        INSERT (
            PRODUCT_ID,
            PRODUCTGROUP_NAME,
            PRODUCT_NAME,
            CREATED_AT,
            UPDATED_AT
        )
        VALUES (
            src.PRODUCT_ID,
            src.PRODUCTGROUP_NAME,
            src.PRODUCT_NAME,
            current_timestamp(),
            current_timestamp()
        )
    """)

def load_product_dim_scd2(spark):

    # Source data
        spark.sql("""
            CREATE OR REPLACE TEMP VIEW src_product AS
            SELECT DISTINCT
                PRODUCT_ID,
                PRODUCT_NAME,
                PRODUCTGROUP_NAME
            FROM gold.market.dim_product
            WHERE PRODUCT_NAME IS NOT NULL
        """)

        # Step 1 - Expire existing current records if Product Group changed
        spark.sql("""
            MERGE INTO gold.market.dim_product_scd2 tgt
            USING src_product src
            ON tgt.product_id = src.product_id
            AND tgt.is_current = true

            WHEN MATCHED
            AND COALESCE(tgt.productgroup_name,'') <> COALESCE(src.productgroup_name,'')
            THEN UPDATE SET
                tgt.is_current = false,
                tgt.effective_end_date = current_timestamp(),
                tgt.updated_at = current_timestamp()
        """)

        # Step 2 - Insert new products and changed products
        spark.sql("""
            INSERT INTO gold.market.dim_product_scd2
            SELECT
                ROW_NUMBER() OVER (ORDER BY src.product_id)
                + COALESCE((SELECT MAX(product_sk)
                            FROM gold.market.dim_product_scd2),0) AS product_sk,

                src.product_id,
                src.productgroup_name,
                src.product_name,
                current_timestamp() AS effective_start_date,
                NULL AS effective_end_date,
                true AS is_current,
                current_timestamp() AS created_at,
                current_timestamp() AS updated_at

            FROM src_product src
            LEFT JOIN gold.market.dim_product_scd2 tgt
                ON src.product_id = tgt.product_id
            AND src.productgroup_name = tgt.productgroup_name
            AND tgt.is_current = true

            WHERE tgt.product_id IS NULL
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

    load_product_dim_scd2(spark)

    log_watermark(spark)

    print("Gold dimension tables loaded successfully")


if __name__ == "__main__":
    main()