

from market_data_terraform_dab.databricks_project.market_analytics.tests.conftest import spark


spark.sql("""
CREATE OR REPLACE TABLE gold.market.DataLake_Price_Prediction_Gold AS
SELECT DISTINCT
dailypricing.DATE_OF_PRICING
,dailypricing.ROW_ID
,dailypricing.STATE_NAME
,dailypricing.MARKET_NAME
,dailypricing.PRODUCTGROUP_NAME
,dailypricing.PRODUCT_NAME
,dailypricing.VARIETY
,dailypricing.ORIGIN
,dailypricing.ARRIVAL_IN_TONNES
,dailypricing.MINIMUM_PRICE
,dailypricing.MAXIMUM_PRICE
,dailypricing.MODAL_PRICE
,geolocation.latitude as MARKET_LATITUDE
,geolocation.longitude as MARKET_LONGITUDE
,geolocation.population as MARKET_POPULATION
,weatherdata.unitOfTemparature as TEMPARATURE_UNIT
,weatherdata.maximumTemparature as MARKET_MAX_TEMPARATURE
,weatherdata.minimumTemparature as MARKET_MIN_TEMPARATURE
,weatherdata.unitOfRainFall as RAINFALL_UNIT
,weatherdata.rainFall as MARKET_DAILY_RAINFALL
,current_timestamp as lakehouse_inserted_date
,current_timestamp as lakehouse_updated_date
FROM silver.market.daily_pricing_silver dailypricing
Full JOIN silver.market.geo_location_silver geolocation
ON STATE_NAME = geolocation.stateName
AND MARKET_NAME = geolocation.marketName
full JOIN silver.market.weather_data_trans weatherdata
ON weatherdata.marketName = MARKET_NAME
AND DATE_OF_PRICING = weatherdata.weatherDate
          """)