import argparse
import logging
import traceback
from pyspark.sql import SparkSession
from pyspark.dbutils import DBUtils


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s"
)

logger = logging.getLogger(__name__)


def get_jdbc_connection_url(
    spark: SparkSession,
    server: str,
    database: str,
    secret_scope: str,
    secret_username_key: str,
    secret_password_key: str,
) -> tuple[str, dict]:

    logger.info("Reading database credentials from secret scope: %s", secret_scope)

    dbutils = DBUtils(spark)

    username = dbutils.secrets.get(scope=secret_scope, key=secret_username_key)
    password = dbutils.secrets.get(scope=secret_scope, key=secret_password_key)

    logger.info("Database credentials retrieved successfully.")

   # jdbc_url = f"jdbc:sqlserver://{server};encrypt=true;databaseName={database}"
    jdbc_url = "jdbc:sqlserver://asqludacoursesserver.database.windows.net;encrypt=true;databaseName=asqludacourses;user=sourcereader;password=DBReader@2024";

    connection_properties = {
        "user": username,
        "password": password,
        "driver": "com.microsoft.sqlserver.jdbc.SQLServerDriver",
    }

    logger.info("JDBC URL created for server: %s, database: %s", server, database)

    return jdbc_url, connection_properties


def fetch_reference_table(
    spark: SparkSession,
    jdbc_url: str,
    table_name: str,
    conn_props: dict
):

    logger.info("Reading source table: %s", table_name)

    df = (
        spark.read
        .format("jdbc")
        .option("url", jdbc_url)
        .option("dbtable", table_name)
        .options(**conn_props)
        .load()
    )

    row_count = df.count()
    logger.info("Successfully read table: %s, row count: %d", table_name, row_count)

    return df


def write_to_bronze(
    df,
    sink_layer: str,
    storage_account: str,
    sink_folder: str,
    table_name: str
):

    table_folder = table_name.replace(".", "/")

    sink_path = (
        f"abfss://{sink_layer}@{storage_account}.dfs.core.windows.net/"
        f"{sink_folder}/{table_folder}"
    )

    logger.info("Writing data to bronze path: %s", sink_path)

    df.write.mode("overwrite").json(sink_path)

    row_count = df.count()
    logger.info("Successfully wrote %d rows to %s", row_count, sink_path)

    return sink_path


def main():

    logger.info("===== Bronze Reference Data Ingestion Started =====")

    parser = argparse.ArgumentParser()

    parser.add_argument("--source_table_name", required=True)
    parser.add_argument("--jdbc_server", required=True)
    parser.add_argument("--jdbc_database", required=True)
    parser.add_argument("--secret_scope", required=True)
    parser.add_argument("--secret_username_key", default="db-username")
    parser.add_argument("--secret_password_key", default="db-password")
    parser.add_argument("--sink_storage_account", required=True)
    parser.add_argument("--sink_layer_name", default="bronze")
    parser.add_argument("--sink_folder_name", default="reference-data")

    args = parser.parse_args()

    logger.info("Source table: %s", args.source_table_name)
    logger.info("JDBC server: %s", args.jdbc_server)
    logger.info("JDBC database: %s", args.jdbc_database)
    logger.info("Secret scope: %s", args.secret_scope)
    logger.info("Sink storage account: %s", args.sink_storage_account)
    logger.info("Sink layer: %s", args.sink_layer_name)
    logger.info("Sink folder: %s", args.sink_folder_name)

    try:
        logger.info("Creating Spark session.")
        spark = SparkSession.builder.getOrCreate()

        logger.info("Building JDBC connection.")
        jdbc_url, conn_props = get_jdbc_connection_url(
            spark,
            args.jdbc_server,
            args.jdbc_database,
            args.secret_scope,
            args.secret_username_key,
            args.secret_password_key,
        )

        logger.info("Fetching source data.")
        df = fetch_reference_table(
            spark,
            jdbc_url,
            args.source_table_name,
            conn_props
        )

        logger.info("Writing data to bronze layer.")
        write_to_bronze(
            df,
            args.sink_layer_name,
            args.sink_storage_account,
            args.sink_folder_name,
            args.source_table_name
        )

        logger.info("===== Bronze Reference Data Ingestion Completed Successfully =====")

    except Exception as e:
        logger.error("===== Bronze Reference Data Ingestion Failed =====")
        logger.error("Error message: %s", str(e))
        logger.error("Full traceback:")
        logger.error(traceback.format_exc())
        raise


if __name__ == "__main__":
    main()