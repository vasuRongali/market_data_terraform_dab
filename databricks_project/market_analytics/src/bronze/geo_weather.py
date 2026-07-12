import argparse
import json
import requests


def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument("--process_name", default="geo_weather_ingest")
    parser.add_argument(
        "--source_api_base_url",
        default="https://archive-api.open-meteo.com/v1/archive?latitude=",
    )
    parser.add_argument(
        "--source_api_options",
        default="&daily=temperature_2m_max,temperature_2m_min,rain_sum",
    )
    parser.add_argument("--source_catalog", default="silver")
    parser.add_argument("--source_schema", default="market")
    parser.add_argument("--source_table", default="geo_location_silver")
    parser.add_argument("--start_date", default="2023-01-01")
    parser.add_argument("--end_date", default="2024-01-01")
    parser.add_argument("--sink_storage_account", required=True)
    parser.add_argument("--sink_layer_name", default="bronze")
    parser.add_argument("--sink_folder_name", default="weather-data")
    return parser.parse_args()


def main():
    args = parse_args()

    weatherDataSourceAPIBaseURL = args.source_api_base_url
    weatherDataSourceAPIURLOptions = args.source_api_options

    weatherDataSinkLayerName = args.sink_layer_name
    weatherDataSinkStorageAccountName = args.sink_storage_account
    weatherDataSinkFolderName = args.sink_folder_name

    weatherDataSinkFolderPath = (
        f"abfss://{weatherDataSinkLayerName}@{weatherDataSinkStorageAccountName}"
        f".dfs.core.windows.net/{weatherDataSinkFolderName}"
    )

    geoLocationsDF = spark.sql(
        f"SELECT latitude, longitude, marketName FROM {args.source_catalog}.{args.source_schema}.{args.source_table} LIMIT 100"
    )
    display(geoLocationsDF.count())

    weatherDataApiResponseList = []
    for geoLocation in geoLocationsDF.collect():
        weatherDataSourceAPIURL = (
            f"{weatherDataSourceAPIBaseURL}"
            f"{geoLocation['latitude']}"
            f"&longitude={geoLocation['longitude']}"
            f"&start_date={args.start_date}"
            f"&end_date={args.end_date}"
            f"{weatherDataSourceAPIURLOptions}"
        )

        weatherDataAPIResponse = requests.get(weatherDataSourceAPIURL, timeout=60).json()

        if isinstance(weatherDataAPIResponse, dict):
            weatherDataAPIResponse["marketName"] = geoLocation["marketName"]
            weatherDataApiResponseList.append(weatherDataAPIResponse)

    geoLocationTempFolderName = "_temp/geo-weather"
    geoLocationTempJSONPath = (
        f"abfss://{weatherDataSinkLayerName}@{weatherDataSinkStorageAccountName}"
        f".dfs.core.windows.net/{geoLocationTempFolderName}/geo_weather_responses.json"
    )

    geoLocationNDJSON = "\n".join(json.dumps(record) for record in weatherDataApiResponseList)
    dbutils.fs.put(geoLocationTempJSONPath, geoLocationNDJSON, overwrite=True)

    df = spark.read.json(geoLocationTempJSONPath)
    df.write.mode("overwrite").json(weatherDataSinkFolderPath)

    dbutils.fs.rm(geoLocationTempJSONPath)


if __name__ == "__main__":
    main()

