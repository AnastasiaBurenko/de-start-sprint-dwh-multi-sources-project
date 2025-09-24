-- Слой cdm, создание витрины

DROP TABLE IF EXISTS cdm.dm_courier_ledger;

CREATE TABLE cdm.dm_courier_ledger(
	id SERIAL PRIMARY KEY,
	courier_id VARCHAR NOT NULL,
	courier_name VARCHAR NOT NULL,
	settlement_year	INT NOT NULL,
	settlement_month INT NOT NULL CHECK (settlement_month BETWEEN 1 AND 12),
	orders_count INT NOT NULL DEFAULT 0 CHECK (orders_count >=0),
	orders_total_sum NUMERIC(14, 2) NOT NULL DEFAULT 0 CHECK (orders_total_sum >=0),
	rate_avg FLOAT NOT NULL CHECK (rate_avg BETWEEN 1 AND 5),
	order_processing_fee NUMERIC NOT NULL DEFAULT 0 CHECK (orders_count >=0) , 
	courier_order_sum NUMERIC(14, 2) NOT NULL DEFAULT 0 CHECK (courier_order_sum >=0),
	courier_tips_sum NUMERIC(14, 2) NOT NULL DEFAULT 0 CHECK (courier_tips_sum >=0),
	courier_reward_sum NUMERIC(14, 2) NOT NULL DEFAULT 0 CHECK (courier_reward_sum >=0),
	CONSTRAINT dm_courier_ledger_courier_settlement_year_month_unique UNIQUE (courier_id, settlement_year, settlement_month)
);


-- Слой dds
-- Создание таблицы измерений курьеров

DROP TABLE IF EXISTS dds.dm_couriers CASCADE;
CREATE TABLE dds.dm_couriers(
	id serial PRIMARY KEY,
	courier_id varchar NOT NULL UNIQUE,
	courier_name varchar NOT NULL	
);


-- Создание таблицы измерений доставок

DROP TABLE IF EXISTS dds.dm_deliveries CASCADE;
CREATE TABLE dds.dm_deliveries(
	id serial PRIMARY KEY,
	order_id int REFERENCES dds.dm_orders(id) NOT NULL,
	delivery_id varchar NOT NULL UNIQUE,
	delivery_ts timestamp NOT NULL,
	courier_id int REFERENCES dds.dm_couriers(id) NOT null,
	address varchar
);

-- Создание таблицы фактов

DROP TABLE IF EXISTS dds.fct_courier_deliveries;
CREATE TABLE dds.fct_courier_deliveries (
	id serial PRIMARY KEY,
	courier_id int REFERENCES dds.dm_couriers(id) NOT NULL,
	delivery_id int REFERENCES dds.dm_deliveries(id) NOT NULL,
	rate int check(rate BETWEEN 1 AND 5) NOT NULL,
	tip_sum numeric(14, 2) DEFAULT 0 CHECK(tip_sum>=0) NOT null
);


--Слой stg
-- api couriers
DROP TABLE IF EXISTS stg.api_couriers CASCADE;
CREATE TABLE stg.deliverysystem_couriers (
	id SERIAL PRIMARY KEY NOT NULL,
	courier_id VARCHAR UNIQUE NOT NULL, 
	object_value VARCHAR NOT null
);

-- api deliveries
DROP TABLE IF EXISTS stg.deliverysystem_deliveries;
CREATE TABLE stg.api_deliveries (
	id SERIAL PRIMARY KEY NOT NULL,
	delivery_id VARCHAR UNIQUE NOT NULL,
	object_value VARCHAR NOT NULL
);

