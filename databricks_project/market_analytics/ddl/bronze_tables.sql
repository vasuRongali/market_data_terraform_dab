create schema if not exists  bronze.masterdata;
CREATE TABLE IF NOT EXISTS bronze.masterdata.country_profile
USING JSON
LOCATION 'abfss://bronze@scmarketdatadev.dfs.core.windows.net/reference-data/masterdata/country_profile';


CREATE TABLE IF NOT EXISTS bronze.masterdata.exchange_rates
USING JSON
LOCATION 'abfss://bronze@scmarketdatadev.dfs.core.windows.net/reference-data/masterdata/exchange_rates';


CREATE TABLE IF NOT EXISTS bronze.masterdata.domestic_product_codes
USING JSON
LOCATION 'abfss://bronze@scmarketdatadev.dfs.core.windows.net/reference-data/masterdata/domestic_product_codes';


CREATE TABLE IF NOT EXISTS bronze.masterdata.global_item_codes
USING JSON
LOCATION 'abfss://bronze@scmarketdatadev.dfs.core.windows.net/reference-data/masterdata/global_item_codes';


CREATE TABLE IF NOT EXISTS bronze.masterdata.market_address
USING JSON
LOCATION 'abfss://bronze@scmarketdatadev.dfs.core.windows.net/reference-data/masterdata/market_address';


create schema if not exists bronze.market;
CREATE TABLE IF NOT EXISTS bronze.market.daily_pricing_bronze
USING DELTA
LOCATION 'abfss://bronze@scmarketdatadev.dfs.core.windows.net/daily-pricing-bronze';

 
CREATE CATALOG IF NOT EXISTS silver
MANAGED LOCATION 'abfss://silver@scmarketdatadev.dfs.core.windows.net/';
create schema if not exists silver.market;
CREATE TABLE IF NOT EXISTS silver.market.daily_pricing_silver (
    DATE_OF_PRICING        DATE,
    ROW_ID                 BIGINT,
    STATE_NAME             STRING,
    MARKET_NAME            STRING,
    PRODUCTGROUP_NAME      STRING,
    PRODUCT_NAME           STRING,
    VARIETY                STRING,
    ORIGIN                 STRING,
    ARRIVAL_IN_TONNES      DECIMAL(18,2),
    MINIMUM_PRICE          DECIMAL(36,2),
    MAXIMUM_PRICE          DECIMAL(36,2),
    MODAL_PRICE            DECIMAL(36,2),
    SOURCE_FILE_LOAD_DATE  TIMESTAMP,
    CREATED_AT             TIMESTAMP,
    UPDATED_AT             TIMESTAMP
)
USING DELTA
LOCATION 'abfss://silver@scmarketdatadev.dfs.core.windows.net/market/daily_pricing_silver';

