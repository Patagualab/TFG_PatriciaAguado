import time, os, json
import numpy as np
import pandas as pd
from datetime import datetime

#################################################################################################################################
# Funciones

def remove_accents (sentence):
    a,b = 'ÁáÉéÍíÓóÚúÜü','AaEeIiOoUuUu'
    trans = str.maketrans(a,b)
    new_sentence = sentence.translate(trans)
    
    return new_sentence

def datetime_to_epoch_ms (timestamp_datetime):
    # Tiempo en milisegundos
    ts_epoch_ms = int(datetime.timestamp(timestamp_datetime))*1000
    
    return ts_epoch_ms

def datetime_to_epoch_ns (timestamp_datetime):
    # Tiempo en nanosegundos
    ts_epoch_ns = int(datetime.timestamp(timestamp_datetime))*1000000000
    
    return ts_epoch_ns

def process_str_opentsdb (tag_or_metric):
    process_str = tag_or_metric.replace(" ", "_").replace("'", "_").replace(":", "_").replace(".", "_").replace(";", "_").replace(",", "_")
    
    return process_str

# Influxdb
def influx_db_json (path_dir_data):
    data = pd.ExcelFile(path_dir_data)
    data = data.parse()
    
    #Almacenará los diccionarios para generar el archivo json
    d = []
    for i in range(len(data)):
        reg = data.iloc[i]
        schema = {"measurement":"energy","fields":{"dev_m":reg['device_measurement']},"time":reg['timestamp'].isoformat()+'Z',"tags":{"device_id":reg['device_id'],"unit_measurement":reg['unit_measurement'],"type_of_tag":reg['type_of_tag'],"information":remove_accents(reg['information'])}}
        d.append(schema)

    return d

# Opentsdb
def opents_db_json (path_dir_data):
    data = pd.ExcelFile(path_dir_data)
    data = data.parse()
    
    #Almacenará los diccionarios para generar el archivo json
    d = []
    for i in range(len(data)):
        reg = data.iloc[i]
        schema = {"metric":"energy","value":reg['device_measurement'],"timestamp":datetime_to_epoch_ms(reg['timestamp']),"tags":{"device_id":process_str_opentsdb(reg['device_id']),"unit_measurement":reg['unit_measurement'],"type_of_tag":process_str_opentsdb(reg['type_of_tag']),"information":process_str_opentsdb(remove_accents(reg['information']))}}
        d.append(schema)

    return d

# Timescaledb
def timescale_db_json (path_dir_data):
    data = pd.ExcelFile(path_dir_data)
    data = data.parse()
    
    #Almacenará los diccionarios para generar el archivo json
    d = []
    for i in range(len(data)):
        reg = data.iloc[i]
        schema = {"device_id":reg['device_id'],"timestamp":reg['timestamp'].isoformat(),"device_measurement":reg['device_measurement'],"unit_measurement":reg['unit_measurement'],"type_of_tag":reg['type_of_tag'],"information":remove_accents(reg['information'])}
        d.append(schema)

    return d

# Kairosdb
def kairos_db_json (path_dir_data):
    data = pd.ExcelFile(path_dir_data)
    data = data.parse()
    
    #Almacenará los diccionarios para generar el archivo json
    d = []
    for i in range(len(data)):
        reg = data.iloc[i]
        schema = {"name":"energy","timestamp":datetime_to_epoch_ms(reg['timestamp']),"type":"double","value":reg['device_measurement'],"tags":{"device_id":reg['device_id'],"unit_measurement":reg['unit_measurement'],"type_of_tag":reg['type_of_tag'],"information":remove_accents(reg['information'])}}
        d.append(schema)
    return d

#################################################################################################################################

# Ficheros de datos
path_dir_data_3 = "./data/gold_data/d_3.xlsx"
path_dir_data_6 = "./data/gold_data/d_6.xlsx"
path_dir_data_10 = "./data/gold_data/d_10.xlsx"
path_dir_data_13 = "./data/gold_data/d_13.xlsx"

# Directorios de ficheros json para cada base de datos
path_dir_influxdb = "./data/json_schemas/influxdb"
path_dir_timescaledb = "./data/json_schemas/timescaledb"
path_dir_opentsdb = "./data/json_schemas/opentsdb"
path_dir_kairosdb = "./data/json_schemas/kairosdb"

path_dir_json = "./data/json_schemas"

if not os.path.exists(path_dir_json):
    os.mkdir(path_dir_json)


#################################################################################################################################
# JSON InfluxDB

if not os.path.exists(path_dir_influxdb):
    os.mkdir(path_dir_influxdb)

json_i3 = influx_db_json(path_dir_data_3)
with open('./data/json_schemas/influxdb/influxdb_3.json', 'w') as file:
    json.dump(json_i3, file, indent=2)
file.close()
print("\n#######   Archivos JSON para 3 meses de InfluxDB generados")

json_i6 = influx_db_json(path_dir_data_6)
with open('./data/json_schemas/influxdb/influxdb_6.json', 'w') as file:
    json.dump(json_i6, file, indent=2)
file.close()
print("\n#######   Archivos JSON para 6 meses de InfluxDB generados")

json_i10 = influx_db_json(path_dir_data_10)
with open('./data/json_schemas/influxdb/influxdb_10.json', 'w') as file:
    json.dump(json_i10, file, indent=2)
file.close()
print("\n#######   Archivos JSON para 10 meses de InfluxDB generados")

json_i13 = influx_db_json(path_dir_data_13)
with open('./data/json_schemas/influxdb/influxdb_13.json', 'w') as file:
    json.dump(json_i13, file, indent=2)
file.close()
print("\n#######   Archivos JSON para 13 meses de InfluxDB generados")
print("\n*********\n")

#################################################################################################################################
# JSON OpenTSDB

if not os.path.exists(path_dir_opentsdb):
    os.mkdir(path_dir_opentsdb)

json_o3 = opents_db_json(path_dir_data_3)
with open('./data/json_schemas/opentsdb/opentsdb_3.json', 'w') as file:
    json.dump(json_o3, file, indent=2)
file.close()
print("\n#######   Archivos JSON para 3 meses de OpenTSDB generados")

json_o6 = opents_db_json(path_dir_data_6)
with open('./data/json_schemas/opentsdb/opentsdb_6.json', 'w') as file:
    json.dump(json_o6, file, indent=2)
file.close()
print("\n#######   Archivos JSON para 6 meses de OpenTSDB generados")

json_o10 = opents_db_json(path_dir_data_10)
with open('./data/json_schemas/opentsdb/opentsdb_10.json', 'w') as file:
    json.dump(json_o10, file, indent=2)
file.close()
print("\n#######   Archivos JSON para 10 meses de OpenTSDB generados")

json_o13 = opents_db_json(path_dir_data_13)
with open('./data/json_schemas/opentsdb/opentsdb_13.json', 'w') as file:
    json.dump(json_o13, file, indent=2)
file.close()
print("\n#######   Archivos JSON para 13 meses de OpenTSDB generados")
print("\n*********\n")

#################################################################################################################################
# JSON TimescaleDB

if not os.path.exists(path_dir_timescaledb):
    os.mkdir(path_dir_timescaledb)

json_t3 = timescale_db_json(path_dir_data_3)
with open('./data/json_schemas/timescaledb/timescaledb_3.json', 'w') as file:
    json.dump(json_t3, file, indent=2)
file.close()
print("\n#######   Archivos JSON para 3 meses de TimescaleDB generados")

json_t6 = timescale_db_json(path_dir_data_6)
with open('./data/json_schemas/timescaledb/timescaledb_6.json', 'w') as file:
    json.dump(json_t6, file, indent=2)
file.close()
print("\n#######   Archivos JSON para 6 meses de TimescaleDB generados")

json_t10 = timescale_db_json(path_dir_data_10)
with open('./data/json_schemas/timescaledb/timescaledb_10.json', 'w') as file:
    json.dump(json_t10, file, indent=2)
file.close()
print("\n#######   Archivos JSON para 10 meses de TimescaleDB generados")

json_t13 = timescale_db_json(path_dir_data_13)
with open('./data/json_schemas/timescaledb/timescaledb_13.json', 'w') as file:
    json.dump(json_t13, file, indent=2)
file.close()
print("\n#######   Archivos JSON para 13 meses de TimescaleDB generados")
print("\n*********\n")

#################################################################################################################################
# JSON KairosDB

if not os.path.exists(path_dir_kairosdb):
    os.mkdir(path_dir_kairosdb)

json_k3 = kairos_db_json(path_dir_data_3)
with open('./data/json_schemas/kairosdb/kairosdb_3.json', 'w') as file:
    json.dump(json_k3, file, indent=2)
file.close()
print("\n#######   Archivos JSON para 3 meses de KairosDB generados")

json_k6 = kairos_db_json(path_dir_data_6)
with open('./data/json_schemas/kairosdb/kairosdb_6.json', 'w') as file:
    json.dump(json_k6, file, indent=2)
file.close()
print("\n#######   Archivos JSON para 6 meses de KairosDB generados")

json_k10 = kairos_db_json(path_dir_data_10)
with open('./data/json_schemas/kairosdb/kairosdb_10.json', 'w') as file:
    json.dump(json_k10, file, indent=2)
file.close()
print("\n#######   Archivos JSON para 10 meses de KairosDB generados")

json_k13 = kairos_db_json(path_dir_data_13)
with open('./data/json_schemas/kairosdb/kairosdb_13.json', 'w') as file:
    json.dump(json_k13, file, indent=2)
file.close()
print("\n#######   Archivos JSON para 13 meses de KairosDB generados")
print("\n*********\nFin de la generación de archivos JSON\n")
#################################################################################################################################
