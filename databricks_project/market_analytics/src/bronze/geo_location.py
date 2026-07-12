import argparse
import json
import requests
import pandas as pds


def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument("--process_name", default="geo_location_ingest")
    parser.add_argument(
        "--source_api_base_url",
        default="https://geocoding-api.open-meteo.com/v1/search?name=",
    )
    parser.add_argument(
        "--source_api_options",
        default="&count=10&language=en&format=json",
    )
    parser.add_argument("--source_catalog", default="gold")
    parser.add_argument("--source_schema", default="market")
    parser.add_argument("--source_table", default="dim_market")
    parser.add_argument("--source_market_limit", type=int, default=10)
    parser.add_argument("--sink_storage_account", required=True)
    parser.add_argument("--sink_layer_name", default="bronze")
    parser.add_argument("--sink_folder_name", default="geo-location")
    return parser.parse_args()


def main():
    args = parse_args()

    geoLocationSourceAPIBaseURL = args.source_api_base_url
    geoLocationSourceAPIURLOptions = args.source_api_options

    geoLocationSinkLayerName = args.sink_layer_name
    geoLocationSinkStorageAccountName = args.sink_storage_account
    geoLocationSinkFolderName = args.sink_folder_name

    geoLocationSinkFolderPath = (
        f"abfss://{geoLocationSinkLayerName}@{geoLocationSinkStorageAccountName}"
        f".dfs.core.windows.net/{geoLocationSinkFolderName}"
    )

    dailyPricingMarketNamesDF = spark.sql(
        f"SELECT MARKET_NAME FROM {args.source_catalog}.{args.source_schema}.{args.source_table} "
        f"LIMIT {args.source_market_limit}"
    )

    marketNames = [
        dailyPricingMarketNames["MARKET_NAME"]
        for dailyPricingMarketNames in dailyPricingMarketNamesDF.collect()
    ]

    geoLocationAPIResponseList = []
    for marketName in marketNames:
        geoLocationSourceAPIURL = (
            f"{geoLocationSourceAPIBaseURL}{marketName}{geoLocationSourceAPIURLOptions}"
        )
        geoLocationAPIResponse = requests.get(geoLocationSourceAPIURL).json()

        if isinstance(geoLocationAPIResponse, dict):
            geoLocationAPIResponseList.append(geoLocationAPIResponse)

    geoLocationTempFolderName = "_temp/geo-location"
    geoLocationTempJSONPath = (
        f"abfss://{geoLocationSinkLayerName}@{geoLocationSinkStorageAccountName}"
        f".dfs.core.windows.net/{geoLocationTempFolderName}/geo_location_responses.json"
    )

    geoLocationNDJSON = "\n".join(json.dumps(record) for record in geoLocationAPIResponseList)
    dbutils.fs.put(geoLocationTempJSONPath, geoLocationNDJSON, overwrite=True)

    geoLocationSparkDF = spark.read.json(geoLocationTempJSONPath)

    (
        geoLocationSparkDF.filter("results.admin1 IS NOT NULL")
        .write.mode("overwrite")
        .json(geoLocationSinkFolderPath)
    )

    dbutils.fs.rm(geoLocationTempJSONPath)

    geoLocationBronzeDF = spark.read.json(geoLocationSinkFolderPath)
    display(geoLocationBronzeDF)


if __name__ == "__main__":
    main()
