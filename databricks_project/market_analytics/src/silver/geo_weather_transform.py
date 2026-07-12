import argparse

import pyspark.sql.functions as F
from pyspark.sql.functions import col, explode


def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument("--catalog", default="silver")
    parser.add_argument("--schema", default="market")
    parser.add_argument("--table", default="weather_data_trans")
    parser.add_argument("--source_layer_name", default="bronze")
    parser.add_argument("--source_storage_account", default="scmarketdatadev")
    parser.add_argument("--source_folder_name", default="weather-data")
    return parser.parse_args()


def main():
    args = parse_args()

    weatherDataSourceFolderPath = (
        f"abfss://{args.source_layer_name}@{args.source_storage_account}"
        f".dfs.core.windows.net/{args.source_folder_name}"
    )

    weatherDataBronzeDF = spark.read.json(weatherDataSourceFolderPath)
    weatherDataTransDF = (
        weatherDataBronzeDF.select(
            col("marketName"),
            col("latitude"),
            col("longitude"),
            explode(col("daily.time")).alias("weatherDate"),
            explode(col("daily.temperature_2m_max")).alias("maximumTemparature"),
            col("daily_units.temperature_2m_max").alias("unitOfTemparature"),
            explode(col("daily.temperature_2m_min")).alias("minimumTemparature"),
            explode(col("daily.rain_sum")).alias("rainFall"),
            col("daily_units.rain_sum").alias("unitOfRainFall"),
            F.monotonically_increasing_id().alias("sequenceId"),
        )
        .limit(2000)
    )

    target_table = f"{args.catalog}.{args.schema}.{args.table}"
    weatherDataTransDF.write.mode("overwrite").saveAsTable(target_table)


if __name__ == "__main__":
    main()

