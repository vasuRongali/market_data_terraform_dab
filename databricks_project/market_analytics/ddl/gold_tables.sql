CREATE CATALOG IF NOT EXISTS gold
MANAGED LOCATION 'abfss://gold@scmarketdatadev.dfs.core.windows.net/';

CREATE SCHEMA IF NOT EXISTS gold.market;

CREATE TABLE IF NOT EXISTS gold.market.dim_state (
    state_id BIGINT,
    state_name STRING,
    created_at TIMESTAMP,
    updated_at TIMESTAMP
)
USING DELTA
LOCATION 'abfss://gold@scmarketdatadev.dfs.core.windows.net/market/dim_state';

CREATE TABLE IF NOT EXISTS gold.market.dim_market (
    market_id BIGINT,
    market_name STRING,
    created_at TIMESTAMP,
    updated_at TIMESTAMP
)
USING DELTA
LOCATION 'abfss://gold@scmarketdatadev.dfs.core.windows.net/market/dim_market';

CREATE TABLE IF NOT EXISTS gold.market.dim_product (
    product_id BIGINT,
    productgroup_name STRING,
    product_name STRING,
    created_at TIMESTAMP,
    updated_at TIMESTAMP
)
USING DELTA
LOCATION 'abfss://gold@scmarketdatadev.dfs.core.windows.net/market/dim_product';


CREATE TABLE IF NOT EXISTS gold.market.dim_variety (
    variety_id BIGINT,
    variety STRING,
    created_at TIMESTAMP,
    updated_at TIMESTAMP
)
USING DELTA
LOCATION 'abfss://gold@scmarketdatadev.dfs.core.windows.net/market/dim_variety';


CREATE TABLE IF NOT EXISTS gold.market.dim_state (
    state_id BIGINT,
    state_name STRING,
    created_at TIMESTAMP,
    updated_at TIMESTAMP
)
USING DELTA
LOCATION 'abfss://gold@scmarketdatadev.dfs.core.windows.net/market/dim_state';

CREATE TABLE IF NOT EXISTS gold.market.dim_date (
    date_id BIGINT,
    calendar_date STRING
)
USING DELTA
LOCATION 'abfss://gold@scmarketdatadev.dfs.core.windows.net/market/dim_date';


CREATE TABLE IF NOT EXISTS gold.market.fact_daily_pricing (
    date_id BIGINT,
    state_id BIGINT,
    market_id BIGINT,
    product_id BIGINT,
    variety BIGINT,
    row_id BIGINT,
    arrival_in_tonnes DECIMAL(18,2),
    maximum_price DECIMAL(36,2),
    minimum_price DECIMAL(36,2),
    modal_price DECIMAL(36,2),
    created_at TIMESTAMP,
    updated_at TIMESTAMP
)
USING DELTA
LOCATION 'abfss://gold@scmarketdatadev.dfs.core.windows.net/market/fact_daily_pricing';


CREATE TABLE IF NOT EXISTS gold.market.dim_product_scd2 (
    product_sk BIGINT,
    product_id BIGINT,
    productgroup_name STRING,
    product_name STRING,
    effective_start_date TIMESTAMP,
    effective_end_date TIMESTAMP,
    is_current BOOLEAN,
    created_at TIMESTAMP,
    updated_at TIMESTAMP
)
USING DELTA
LOCATION 'abfss://gold@scmarketdatadev.dfs.core.windows.net/market/dim_product_scd2';

