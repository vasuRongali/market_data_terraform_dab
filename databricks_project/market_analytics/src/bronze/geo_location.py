# Databricks notebook source
# =============================================================================
# Ingest GeoLocation API Source Data - Solution
#
# Source API URL example:
#   https://geocoding-api.open-meteo.com/v1/search?name=kovilpatti&count=10&language=en&format=json
#
# JSON Target File Path:
#   abfss://bronze@datalakestorageaccountname.dfs.core.windows.net/geo-location/
# =============================================================================

import requests
import json
import pandas as pds
from pyspark.sql.types import *
from pyspark.sql.functions import col, array_contains

# -----------------------------------------------------------------------------
# Configuration
# -----------------------------------------------------------------------------
geoLocationSourceAPIBaseURL = "https://geocoding-api.open-meteo.com/v1/search?name="
geoLocationSourceAPIURLOptions = "&count=10&language=en&format=json"

geoLocationSinkLayerName = 'bronze'
geoLocationSinkStorageAccountName = 'scmarketdatadev'
geoLocationSinkFolderName = 'geo-location'

geoLocationSinkFolderPath = (
    f"abfss://{geoLocationSinkLayerName}@{geoLocationSinkStorageAccountName}"  # Ensure storage account key is configured in cluster settings
    f".dfs.core.windows.net/{geoLocationSinkFolderName}"  # Ensure storage account key is configured in cluster settings
)

# -----------------------------------------------------------------------------
# Quick test call to the GeoLocation API (single market example)
# -----------------------------------------------------------------------------
geoLocationSourceAPIURL = (
    "https://geocoding-api.open-meteo.com/v1/search?name=kovilpatti&count=10&language=en&format=json"
)
geoLocationAPIResponse = requests.get(geoLocationSourceAPIURL).json()
geoLocationPandasDF = pds.DataFrame(geoLocationAPIResponse)
geoLocationSparkDF = spark.createDataFrame(geoLocationPandasDF)

# -----------------------------------------------------------------------------
# Get distinct market names from the gold dimension table
# -----------------------------------------------------------------------------
dailyPricingMarketNamesDF = spark.sql(
    "SELECT MARKET_NAME from gold.market.dim_market limit 10 "
)

marketNames = [
    dailyPricingMarketNames["MARKET_NAME"]
    for dailyPricingMarketNames in dailyPricingMarketNamesDF.collect()
]

# -----------------------------------------------------------------------------
# Call the GeoLocation API for every market name and collect responses
# -----------------------------------------------------------------------------
geoLocationAPIResponseList = []
for marketName in marketNames:

    geoLocationSourceAPIURL = (
        f"{geoLocationSourceAPIBaseURL}{marketName}{geoLocationSourceAPIURLOptions}"
    )
    geoLocationAPIResponse = requests.get(geoLocationSourceAPIURL).json()

    if isinstance(geoLocationAPIResponse, dict):
        geoLocationAPIResponseList.append(geoLocationAPIResponse)

# -----------------------------------------------------------------------------
# Convert API responses into a Spark DataFrame and write to the bronze layer
# -----------------------------------------------------------------------------
 
geoLocationTempFolderName = "_temp/geo-location"
geoLocationTempJSONPath = (
    f"abfss://{geoLocationSinkLayerName}@{geoLocationSinkStorageAccountName}"
    f".dfs.core.windows.net/{geoLocationTempFolderName}/geo_location_responses.json"
)

geoLocationNDJSON = "\n".join(json.dumps(record) for record in geoLocationAPIResponseList)
dbutils.fs.put(geoLocationTempJSONPath, geoLocationNDJSON, overwrite=True)

geoLocationSparkDF = spark.read.json(geoLocationTempJSONPath)

(geoLocationSparkDF
 .filter("results.admin1 IS NOT NULL")
 .write
 .mode("overwrite")
 .mode('overwrite').json(geoLocationSinkFolderPath))

dbutils.fs.rm(geoLocationTempJSONPath)

# -----------------------------------------------------------------------------
# Read back the bronze data to validate the write
# -----------------------------------------------------------------------------
geoLocationBronzeDF = (
    spark
    .read
    .json(geoLocationSinkFolderPath)
)

display(geoLocationBronzeDF)
