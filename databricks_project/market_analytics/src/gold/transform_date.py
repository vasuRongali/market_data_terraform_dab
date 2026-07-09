import argparse
from pyspark.sql import SparkSession
from pyspark.sql.functions import explode, sequence, to_date, row_number, date_format
from pyspark.sql.window import Window


def load_dim_date(spark: SparkSession):
    """Generate Date Dimension for the year 2023."""

    dim_date_df = (
        spark.sql("""
            SELECT explode(
                sequence(
                    to_date('2023-01-01'),
                    to_date('2023-12-31'),
                    interval 1 day
                )
            ) AS calendar_date
        """)
        .withColumn(
            "date_id",
            row_number().over(Window.orderBy("calendar_date"))
        )
        .withColumn(
            "calendar_date",
            date_format("calendar_date", "dd/MM/yyyy")
        )
    )

    (
        dim_date_df
        .write
        .mode("overwrite")
        .insertInto("gold.market.dim_date")
    )

    print(f"Loaded {dim_date_df.count()} records into gold.market.dim_date")


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--catalog", default="gold")
    args = parser.parse_args()

    spark = SparkSession.builder.getOrCreate()

    spark.sql(f"USE CATALOG {args.catalog}")

    load_dim_date(spark)


if __name__ == "__main__":
    main()