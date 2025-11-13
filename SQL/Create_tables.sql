-- Dimension Table: Date
CREATE TABLE IF NOT EXISTS dim_date (
    date_key SERIAL PRIMARY KEY,
    year_of_trade INT NOT NULL UNIQUE
);

-- Dimension Table: Flow (Imports/Exports)
CREATE TABLE IF NOT EXISTS dim_flow (
    flow_key SERIAL PRIMARY KEY,
    flow_description VARCHAR(50) UNIQUE NOT NULL
);

-- Dimension Table: Partner Country
CREATE TABLE IF NOT EXISTS dim_country (
    partner_country_key SERIAL PRIMARY KEY,
    partner_country_name VARCHAR(255) NOT NULL UNIQUE,
    partner_country_iso VARCHAR(3) ,
    partner_code INT UNIQUE
);

-- Dimension Table: Commodity
CREATE TABLE IF NOT EXISTS dim_commodity (
    commodity_key SERIAL PRIMARY KEY,
    commodity_description TEXT NOT NULL UNIQUE
);

-- Dimension Table: Economic Indicator
CREATE TABLE IF NOT EXISTS dim_indicator (
    indicator_key SERIAL PRIMARY KEY,
    indicator_code VARCHAR(50) NOT NULL UNIQUE,
    indicator_name TEXT
);

-- Fact Table: Trade Data
CREATE TABLE IF NOT EXISTS fact_trade (
    fact_key SERIAL PRIMARY KEY,
    date_key_fk INT REFERENCES dim_date(date_key),
    partner_country_key_fk INT REFERENCES dim_country(partner_country_key),
    commodity_key_fk INT REFERENCES dim_commodity(commodity_key),
    flow_key_fk INT REFERENCES dim_flow(flow_key),
    trade_value DECIMAL(30, 5),
    net_weight DECIMAL(30, 5)
);

-- Fact Table: Economic Data
CREATE TABLE IF NOT EXISTS fact_economy (
    fact_key SERIAL PRIMARY KEY,
    date_key_fk INT REFERENCES dim_date(date_key),
    indicator_key_fk INT REFERENCES dim_indicator(indicator_key),
    indicator_value DECIMAL(30, 5)
);