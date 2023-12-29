CREATE EXTENSION IF NOT EXISTS timescaledb;
CREATE EXTENSION IF NOT EXISTS pg_stat_statements;

CREATE TABLE IF NOT EXISTS devices (
	device_id TEXT NOT NULL,
	unit_measurement TEXT NOT NULL CHECK (unit_measurement IN ('MWh','kWh','Valor')),
	information TEXT NOT NULL,
	PRIMARY KEY (device_id)
);

CREATE TABLE IF NOT EXISTS energy (
	device_id TEXT NOT NULL,
	timestamp TIMESTAMPTZ NOT NULL,
	device_measurement DOUBLE PRECISION NOT NULL,
	type_of_tag TEXT NOT NULL,
	PRIMARY KEY (device_id,timestamp)
);

SELECT create_hypertable('energy', 'timestamp', chunk_time_interval => INTERVAL '1 month', if_not_exists => TRUE);
CREATE INDEX IF NOT EXISTS idx_device_id ON energy (device_id);
ANALYZE energy;
