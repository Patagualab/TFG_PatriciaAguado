CREATE TABLE IF NOT EXISTS m_data (
	id_exp INTEGER NOT NULL,
	db_exp VARCHAR NOT NULL CHECK (db_exp IN ('influxdb','timescaledb','opentsdb','kairosdb')),
	size_exp INTEGER NOT NULL CHECK (size_exp IN (3,6,10,13)),
	time_exp_ms DECIMAL(15,3) NOT NULL,
	PRIMARY KEY (id_exp,db_exp)
);

CREATE TABLE IF NOT EXISTS m_space (
	id_exp INTEGER NOT NULL,
	db_exp VARCHAR NOT NULL CHECK (db_exp IN ('jsontimescale','jsoninflux','jsonopen','jsonkairos','influxdb','timescaledb','opentsdb','kairosdb')),
	size_exp INTEGER NOT NULL CHECK (size_exp IN (3,6,10,13)),
	space_exp_mb DECIMAL(15,3) NOT NULL,
	PRIMARY KEY (id_exp,db_exp)
);

CREATE TABLE IF NOT EXISTS m_query_3 (
	id_exp INTEGER NOT NULL,
	db_exp VARCHAR NOT NULL CHECK (db_exp IN ('influxdb','timescaledb','opentsdb','kairosdb')),
	type_q VARCHAR NOT NULL CHECK (type_q IN ('ep','agg','gb','tr')),
	n_q INTEGER NOT NULL CHECK (n_q IN (1,2,3,4,5,6,7,8,9,10)),
	n_var INTEGER NOT NULL CHECK (n_var IN (1,5,20)),
	size_q VARCHAR NOT NULL CHECK (size_q IN ('all','hour','day','week','month','dayhour','monthweek')),
	time_exp_ms DECIMAL(15,3) NOT NULL,
	PRIMARY KEY (id_exp,db_exp)
);

CREATE TABLE IF NOT EXISTS m_query_6 (
	id_exp INTEGER NOT NULL,
	db_exp VARCHAR NOT NULL CHECK (db_exp IN ('influxdb','timescaledb','opentsdb','kairosdb')),
	type_q VARCHAR NOT NULL CHECK (type_q IN ('ep','agg','gb','tr')),
	n_q INTEGER NOT NULL CHECK (n_q IN (1,2,3,4,5,6,7,8,9,10)),
	n_var INTEGER NOT NULL CHECK (n_var IN (1,5,20)),
	size_q VARCHAR NOT NULL CHECK (size_q IN ('all','hour','day','week','month','dayhour','monthweek')),
	time_exp_ms DECIMAL(15,3) NOT NULL,
	PRIMARY KEY (id_exp,db_exp)
);

CREATE TABLE IF NOT EXISTS m_query_10 (
	id_exp INTEGER NOT NULL,
	db_exp VARCHAR NOT NULL CHECK (db_exp IN ('influxdb','timescaledb','opentsdb','kairosdb')),
	type_q VARCHAR NOT NULL CHECK (type_q IN ('ep','agg','gb','tr')),
	n_q INTEGER NOT NULL CHECK (n_q IN (1,2,3,4,5,6,7,8,9,10)),
	n_var INTEGER NOT NULL CHECK (n_var IN (1,5,20)),
	size_q VARCHAR NOT NULL CHECK (size_q IN ('all','hour','day','week','month','dayhour','monthweek')),
	time_exp_ms DECIMAL(15,3) NOT NULL,
	PRIMARY KEY (id_exp,db_exp)
);

CREATE TABLE IF NOT EXISTS m_query_13 (
	id_exp INTEGER NOT NULL,
	db_exp VARCHAR NOT NULL CHECK (db_exp IN ('influxdb','timescaledb','opentsdb','kairosdb')),
	type_q VARCHAR NOT NULL CHECK (type_q IN ('ep','agg','gb','tr')),
	n_q INTEGER NOT NULL CHECK (n_q IN (1,2,3,4,5,6,7,8,9,10)),
	n_var INTEGER NOT NULL CHECK (n_var IN (1,5,20)),
	size_q VARCHAR NOT NULL CHECK (size_q IN ('all','hour','day','week','month','dayhour','monthweek')),
	time_exp_ms DECIMAL(15,3) NOT NULL,
	PRIMARY KEY (id_exp,db_exp)
);