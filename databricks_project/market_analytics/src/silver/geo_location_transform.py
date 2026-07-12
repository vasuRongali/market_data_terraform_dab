import argparse

from pyspark.sql.functions import col, explode, monotonically_increasing_id


def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument("--catalog", default="silver")
    parser.add_argument("--schema", default="market")
    parser.add_argument("--table", default="geo_location_silver")
    parser.add_argument("--source_layer_name", default="bronze")
    parser.add_argument("--source_storage_account", default="scmarketdatadev")
    parser.add_argument("--source_folder_name", default="geo-location")
    return parser.parse_args()


def main():
    args = parse_args()

    geoLocationSourceFolderPath = (
        f"abfss://{args.source_layer_name}@{args.source_storage_account}"
        f".dfs.core.windows.net/{args.source_folder_name}"
    )

    geoLocationBronzeDF = spark.read.json(geoLocationSourceFolderPath)
    display(geoLocationBronzeDF)

    geoLocationSilverDF = (
        geoLocationBronzeDF.select(
            col("results.admin1").alias("stateName"),
            col("results.admin2").alias("districtName"),
            col("results.country").alias("countryName"),
            col("results.latitude").alias("latitude"),
            col("results.longitude").alias("longitude"),
            col("results.name").alias("marketName"),
            col("results.population").alias("population"),
        )
    )
    display(geoLocationSilverDF)

    geoLocationStateTransDF = (
        geoLocationSilverDF.select(
            explode("stateName").alias("stateName"),
            monotonically_increasing_id().alias("stateSequenceId"),
        )
    )

    geoLocationDistrictTransDF = (
        geoLocationSilverDF.select(
            explode("districtName").alias("districtName"),
            monotonically_increasing_id().alias("districtSequenceId"),
        )
    )

    geoLocationCountryTransDF = (
        geoLocationSilverDF.select(
            explode("countryName").alias("countryName"),
            monotonically_increasing_id().alias("countryNameSequenceId"),
        )
    )

    geoLocationLatitudeTransDF = (
        geoLocationSilverDF.select(
            explode("latitude").alias("latitude"),
            monotonically_increasing_id().alias("latitudeSequenceId"),
        )
    )

    geoLocationLongitudeTransDF = (
        geoLocationSilverDF.select(
            explode("longitude").alias("longitude"),
            monotonically_increasing_id().alias("longitudeSequenceId"),
        )
    )

    geoLocationMarkeTransDF = (
        geoLocationSilverDF.select(
            explode("marketName").alias("marketName"),
            monotonically_increasing_id().alias("marketSequenceId"),
        )
    )

    geoLocationPopulationTransDF = (
        geoLocationSilverDF.select(
            explode("population").alias("population"),
            monotonically_increasing_id().alias("populationSequenceId"),
        )
    )

    geoLocationSilverTransDF = (
        geoLocationStateTransDF.join(
            geoLocationDistrictTransDF,
            col("stateSequenceId") == col("districtSequenceId"),
        )
        .join(
            geoLocationCountryTransDF,
            col("stateSequenceId") == col("countryNameSequenceId"),
        )
        .join(
            geoLocationLatitudeTransDF,
            col("stateSequenceId") == col("latitudeSequenceId"),
        )
        .join(
            geoLocationLongitudeTransDF,
            col("stateSequenceId") == col("longitudeSequenceId"),
        )
        .join(geoLocationMarkeTransDF, col("stateSequenceId") == col("marketSequenceId"))
        .join(
            geoLocationPopulationTransDF,
            col("stateSequenceId") == col("populationSequenceId"),
        )
        .select(
            col("stateName"),
            col("districtName"),
            col("countryName"),
            col("latitude"),
            col("longitude"),
            col("marketName"),
            col("population"),
        )
    )

    target_table = f"{args.catalog}.{args.schema}.{args.table}"
    geoLocationSilverTransDF.write.mode("overwrite").saveAsTable(target_table)


if __name__ == "__main__":
    main()
 