import psycopg2
import requests
import json, os, re, time, sys
import docker
from datetime import datetime

#############################################################################
# Opentsdb
db_container_opentsdb="opents_db_container"

#############################################################################
# Contenedor PostgresDB 
db_container_psql="postgres_db_container"

# PostgresDB
db_host_psql="localhost"
db_port_psql="5432"
db_name_psql="postgres_db"
db_user_psql="admin"
db_pssw_psql="admin"

#############################################################################
# Identificadores y consultas

list_device = ["AInt_PS_AnlzRed21_En1",
"AInt_PS_AnlzRed41_En1",
"AInt_PS_AnlzRed29_AI65",
"AInt_P1_AnlzRed01_AI65",
"AInt_P2_AnlzRed10_AI65",
"AInt_PS_AnlzRed28_AI65",
"AInt_PS_AnlzRed35_AI65",
"AInt_PS_AnlzRed39_En1",
"AInt_PB_AnlzRed07_AI65",
"AInt_PB_AnlzRed13_AI65",
"AInt_PB_AnlzRed14_AI65",
"AInt_P1_AnlzRed07_AI65",
"AInt_P1_AnlzRed11_AI65",
"AInt_P1_AnlzRed24_AI65",
"AInt_P1_AnlzRed25_AI65",
"AInt_P2_AnlzRed05_AI65",
"AInt_P2_AnlzRed06_AI65",
"AInt_P2_AnlzRed26_AI65",
"AInt_P2_AnlzRed27_AI65",
"AInt_PS_AnlzRed23_PotT"]

queries = ["Obtener para un determinado sensor una medicion de energia en un dia y una hora determinados",
            "Obtener para un determinado sensor la minima medicion de energia",
            "Obtener para un determinado sensor la maxima medicion de energia",
            "Obtener para un determinado sensor la media de sus mediciones de energia",
            "Obtener para un determinado sensor la desviacion estandar de sus mediciones de energia",
            "Obtener para un determinado sensor el conteo de sus mediciones de energia",
            "Obtener para un determinado sensor los valores sin duplicados que toman sus mediciones de energia",
            "Obtener para un determinado sensor la media de sus mediciones de energia por horas",
            "Obtener para un determinado sensor la media de sus mediciones de energia por semanas",
            "Obtener para un determinado sensor la media de sus mediciones de energia por meses",
            "Obtener para un determinado sensor los valores atipicos de sus mediciones de energia",
            "Obtener para un sensor determinado las mediciones de energia en un rango de tiempo determinado, un dia",
            "Obtener para un sensor determinado las mediciones de energia en un rango de tiempo determinado, un dia y unas horas",
            "Obtener para un sensor determinado las mediciones de energia en un rango de tiempo determinado, un mes",
            "Obtener para un sensor determinado las mediciones de energia en un rango de tiempo determinado, un mes y unas semanas"
            ]

#############################################################################
# Funciones

# Cambia el timestamp en formato datetime a milisegundos de tiempo unix
def datetime_to_epoch_ms (timestamp_datetime):
    # Tiempo en milisegundos
    ts_epoch_ms = int(datetime.timestamp(timestamp_datetime))*1000
    
    return ts_epoch_ms

# Procesa la salida del comando pasado al contenedor de docker
def process_space_db (str_space):
    #Expresion regular para el tamaño de los datos en influxdb
    r_space = re.compile('\d+(\.\d+)?')
    str_space = r_space.search(str_space).group(0)
    # Convertir cantidad de B a MB
    space = float(str_space)/1024
        
    return space

# Cambia el timestamp en formato segundos de unix a marca de tiempo datetime
def epoch_s_to_datetime(timestamp_s):
    datetime_ts = datetime.fromtimestamp(int(timestamp_s))
    datetime_ts_str = datetime_ts.strftime("%Y-%m-%d %H:%M:%S")
    
    return datetime_ts_str

# Cambia el timestamp en formato segundos de unix a marca de tiempo datetime interpretado como UTC
def epoch_s_to_datetime_utc(timestamp_s):
    datetime_ts = datetime.utcfromtimestamp(int(timestamp_s))
    datetime_ts_str = datetime_ts.strftime("%Y-%m-%d %H:%M:%S")
    
    return datetime_ts_str

# Procesar el identificador del sensor para que coincida con el de las demás bases de datos
def process_str_opentsdb (tag_or_metric):
    process_str = tag_or_metric.replace("_", "'")
    
    return process_str

# Crea un archivo json con el array de resultados
def export_results_tojson (results, database, data_size, n_vars):
    path_dir_results = "./docker/results"
    path_dir_results_db = path_dir_results + "/" + database

    if not os.path.exists(path_dir_results):
        os.mkdir(path_dir_results)
    if not os.path.exists(path_dir_results_db):
        os.mkdir(path_dir_results_db)

    path_file_result = path_dir_results_db + "/" + database + "_" + data_size + "data_" + n_vars + "var.json"

    with open(path_file_result, 'w') as file:
        json.dump(results, file, indent=2)
    file.close()

#############################################################################
# Datos
dir_json_path="./data/json_schemas/opentsdb"

# Conexiones

# PostgresDB
postgresdb_conn = psycopg2.connect(
    host=db_host_psql,
    port=db_port_psql,
    user=db_user_psql,
    password=db_pssw_psql,
    database=db_name_psql
)
pg_cursor = postgresdb_conn.cursor()

# Opentsdb
url_opentsdb_post="http://localhost:4242/api/put"
url_opentsdb_query="http://localhost:4242/api/query"

##########################################################################################################################################################
##########################################################################################################################################################
# Índices

pg_cursor.execute("SELECT MAX(id_exp) FROM m_data WHERE db_exp = 'opentsdb'")
id_exp_data = pg_cursor.fetchall()[0][0]
id_exp_data = 0 if id_exp_data == None else int(id_exp_data)

pg_cursor.execute("SELECT MAX(id_exp) FROM m_space WHERE db_exp = 'opentsdb'")
id_exp_space = pg_cursor.fetchall()[0][0]
id_exp_space = 0 if id_exp_space == None else int(id_exp_space)

pg_cursor.execute("SELECT MAX(id_exp) FROM m_query_3 WHERE db_exp = 'opentsdb'")
id_exp_query_3 = pg_cursor.fetchall()[0][0]
id_exp_query_3 = 0 if id_exp_query_3 == None else int(id_exp_query_3)

pg_cursor.execute("SELECT MAX(id_exp) FROM m_query_6 WHERE db_exp = 'opentsdb'")
id_exp_query_6 = pg_cursor.fetchall()[0][0]
id_exp_query_6 = 0 if id_exp_query_6 == None else int(id_exp_query_6)

pg_cursor.execute("SELECT MAX(id_exp) FROM m_query_10 WHERE db_exp = 'opentsdb'")
id_exp_query_10 = pg_cursor.fetchall()[0][0]
id_exp_query_10 = 0 if id_exp_query_10 == None else int(id_exp_query_10)

pg_cursor.execute("SELECT MAX(id_exp) FROM m_query_13 WHERE db_exp = 'opentsdb'")
id_exp_query_13 = pg_cursor.fetchall()[0][0]
id_exp_query_13 = 0 if id_exp_query_13 == None else int(id_exp_query_13) 


db_exp = "opentsdb"
size_exp = int(sys.argv[-1])

if size_exp == 3:
    dir_data_path = dir_json_path + "/opentsdb_3.json"
    m_query = "m_query_3"
    id_exp_query = id_exp_query_3
elif size_exp == 6:
    dir_data_path = dir_json_path + "/opentsdb_6.json"
    m_query = "m_query_6"
    id_exp_query = id_exp_query_6
elif size_exp == 10:
    dir_data_path = dir_json_path + "/opentsdb_10.json"
    m_query = "m_query_10"
    id_exp_query = id_exp_query_10
else:
    dir_data_path = dir_json_path + "/opentsdb_13.json"
    m_query = "m_query_13"
    id_exp_query = id_exp_query_13

# Consultas carga de datos
with open(dir_data_path, "r") as json_file:
    data = json.load(json_file)

time_exp_ms_data_total = 0.0

# Si se aumenta falla la escritura
size_batch = 20
batchs = [data[i:i + size_batch] for i in range(0, len(data), size_batch)]

for batch in batchs:
    while True:
        try:
            ts = time.time_ns()
            query_mdata = requests.post(url_opentsdb_post, json=batch)
            te = time.time_ns()
            time_exp_ms_data = (te-ts)/1_000_000

            # Verificar respuesta
            query_mdata.raise_for_status()
            if (query_mdata.status_code == 200 or query_mdata.status_code == 204):
                time_exp_ms_data_total += time_exp_ms_data
                #print("Datos enviados con éxito.")
                break

        except requests.exceptions.RequestException as e:
            #print(f'Error al realizar la solicitud: {e}')
            pass

        # Esperar antes de realizar el siguiente intento
        time.sleep(0.2)

time.sleep(20)
# Almacenar resultado
result_query_mdata = (id_exp_data+1,db_exp,size_exp,round(time_exp_ms_data_total,3))
#print("\n************************************************************************")
#print("tables ---> m_data")
#print(result_query_mdata)

insert_query_mdata = "INSERT INTO m_data (id_exp,db_exp,size_exp,time_exp_ms) VALUES (%s, %s, %s, %s)"
pg_cursor.execute(insert_query_mdata,result_query_mdata)
postgresdb_conn.commit()

##########################################################################################################################################################
##########################################################################################################################################################
'''q = {
  'start': '0',
  'end' : 'now',
  "queries": [
        {
        "aggregator": "sum",
        "metric": "energy",
        "downsample": "0all-count"
        }
    ]
}

select_query_mquery = requests.post(url_opentsdb_query, data=json.dumps(q))
time_exp_ms_query = (te-ts)/1_000_000
result_query = select_query_mquery.json()
#print(str(result_query[0]["dps"]['0']))
n_regs = result_query[0]["dps"]['0']
with open("./resssssssssOpen.txt", 'a') as archivo:
    archivo.write(str(size_exp) + "   " +str(n_regs) + "\n**************\n")'''

##########################################################################################################################################################
# Consultas latencia de consultas

#print("\n************************************************************************")
#print("\ntables ---> m_query")

if int(sys.argv[1]) == 1:
    devices_ids = list_device[:1]
elif int(sys.argv[1]) == 5:
    devices_ids = list_device[:5]
else:
    devices_ids = list_device[:20]

type_q = ["ep","agg","gb","tr"]
size_q = ["all","hour","day","week","month","dayhour","monthweek"]

query_results = []

# Consulta instante de tiempo, punto exacto, n_q=1 #############################################################################
timestamp_ep_start = "2019-03-02T10:21:19"
timestamp_ep_end = "2019-03-02T10:21:20"
n_q = 1

query = {
    'start': datetime_to_epoch_ms(datetime.fromisoformat(timestamp_ep_start)),
    'end' : datetime_to_epoch_ms(datetime.fromisoformat(timestamp_ep_end)),
    "queries": [
        {
        "aggregator": "none",
        "metric": "energy",
        "tags": {
            "device_id": "|".join(devices_ids)
        }
        }
    ]
}

ts = time.time_ns()
select_query_mquery = requests.post(url_opentsdb_query, data=json.dumps(query))
te = time.time_ns()
time_exp_ms_query = (te-ts)/1_000_000
result_query = select_query_mquery.json()
#print(json.dumps(result_query, indent=2))

q1_result = []
for r in result_query:
    for ts, v in r["dps"].items():
        schema = {"device_id":process_str_opentsdb(r["tags"]["device_id"]),"timestamp":epoch_s_to_datetime(ts),"value":v}
        q1_result.append(schema)
query_results.append({"query":"q_1","size_q":size_exp,"info_query":queries[0],"results":q1_result})
'''for i in q1_result:
    print(i)'''

result_query_mquery = (id_exp_query+1,db_exp,type_q[0],n_q,len(devices_ids),size_q[0],round(time_exp_ms_query,3))
#print(result_query_mquery)
insert_query_mquery = f"INSERT INTO {m_query} (id_exp,db_exp,type_q,n_q,n_var,size_q,time_exp_ms) VALUES (%s, %s, %s, %s, %s, %s, %s)"
pg_cursor.execute(insert_query_mquery,result_query_mquery)
postgresdb_conn.commit()

#print("\n************************************************************************")
# Consulta analítica, min n_q=2 ################################################################################################
n_q = 2

query = {
    'start': '0',
    'end' : 'now',
    "queries": [
        {
        "aggregator": "min",
        "metric": "energy",
        "downsample": "0all-min",
        "tags": {
            "device_id": "|".join(devices_ids)
        }
        }
    ]
}

ts = time.time_ns()
select_query_mquery = requests.post(url_opentsdb_query, data=json.dumps(query))
te = time.time_ns()
time_exp_ms_query = (te-ts)/1_000_000
result_query = select_query_mquery.json()
#print(json.dumps(result_query, indent=2))

q2_result = []
for r in result_query:
    for ts, v in r["dps"].items():
        schema = {"device_id":process_str_opentsdb(r["tags"]["device_id"]),"timestamp":"null","value":v}
        q2_result.append(schema)
query_results.append({"query":"q_2","size_q":size_exp,"info_query":queries[1],"results":q2_result})
'''for i in q2_result:
    print(i)'''

result_query_mquery = (id_exp_query+2,db_exp,type_q[1],n_q,len(devices_ids),size_q[0],round(time_exp_ms_query,3))
#print(result_query_mquery)
insert_query_mquery = f"INSERT INTO {m_query} (id_exp,db_exp,type_q,n_q,n_var,size_q,time_exp_ms) VALUES (%s, %s, %s, %s, %s, %s, %s)"
pg_cursor.execute(insert_query_mquery,result_query_mquery)
postgresdb_conn.commit()

#print("\n************************************************************************")
# Consulta analítica, max n_q=3 ################################################################################################
n_q = 3

query = {
    'start': '0',
    'end' : 'now',
    "queries": [
        {
        "aggregator": "max",
        "metric": "energy",
        "downsample": "0all-max",
        "tags": {
            "device_id": "|".join(devices_ids)
        }
        }
    ]
}

ts = time.time_ns()
select_query_mquery = requests.post(url_opentsdb_query, data=json.dumps(query))
te = time.time_ns()
time_exp_ms_query = (te-ts)/1_000_000
result_query = select_query_mquery.json()
#print(json.dumps(result_query, indent=2))

q3_result = []
for r in result_query:
    for ts, v in r["dps"].items():
        schema = {"device_id":process_str_opentsdb(r["tags"]["device_id"]),"timestamp":"null","value":v}
        q3_result.append(schema)
query_results.append({"query":"q_3","size_q":size_exp,"info_query":queries[2],"results":q3_result})
'''for i in q3_result:
    print(i)'''

result_query_mquery = (id_exp_query+3,db_exp,type_q[1],n_q,len(devices_ids),size_q[0],round(time_exp_ms_query,3))
#print(result_query_mquery)
insert_query_mquery = f"INSERT INTO {m_query} (id_exp,db_exp,type_q,n_q,n_var,size_q,time_exp_ms) VALUES (%s, %s, %s, %s, %s, %s, %s)"
pg_cursor.execute(insert_query_mquery,result_query_mquery)
postgresdb_conn.commit()

#print("\n************************************************************************")
# Consulta analítica, avg n_q=4 ################################################################################################
n_q = 4

query = {
    'start': '0',
    'end' : 'now',
    "queries": [
        {
        "aggregator": "avg",
        "metric": "energy",
        "downsample": "0all-avg",
        "tags": {
            "device_id": "|".join(devices_ids)
        }
        }
    ]
}

ts = time.time_ns()
select_query_mquery = requests.post(url_opentsdb_query, data=json.dumps(query))
te = time.time_ns()
time_exp_ms_query = (te-ts)/1_000_000
result_query = select_query_mquery.json()
#print(json.dumps(result_query, indent=2))

q4_result = []
for r in result_query:
    for ts, v in r["dps"].items():
        schema = {"device_id":process_str_opentsdb(r["tags"]["device_id"]),"timestamp":"null","value":v}
        q4_result.append(schema)
query_results.append({"query":"q_4","size_q":size_exp,"info_query":queries[3],"results":q4_result})
'''for i in q4_result:
    print(i)'''

result_query_mquery = (id_exp_query+4,db_exp,type_q[1],n_q,len(devices_ids),size_q[0],round(time_exp_ms_query,3))
#print(result_query_mquery)
insert_query_mquery = f"INSERT INTO {m_query} (id_exp,db_exp,type_q,n_q,n_var,size_q,time_exp_ms) VALUES (%s, %s, %s, %s, %s, %s, %s)"
pg_cursor.execute(insert_query_mquery,result_query_mquery)
postgresdb_conn.commit()

#print("\n************************************************************************")
# Consulta analítica, sttdesv n_q=5 ############################################################################################
n_q = 5

query = {
    'start': '0',
    'end' : 'now',
    "queries": [
        {
        "aggregator": "dev",
        "metric": "energy",
        "downsample": "0all-dev",
        "tags": {
            "device_id": "|".join(devices_ids)
        }
        }
    ]
}

ts = time.time_ns()
select_query_mquery = requests.post(url_opentsdb_query, data=json.dumps(query))
te = time.time_ns()
time_exp_ms_query = (te-ts)/1_000_000
result_query = select_query_mquery.json()
#print(json.dumps(result_query, indent=2))

q5_result = []
for r in result_query:
    for ts, v in r["dps"].items():
        schema = {"device_id":process_str_opentsdb(r["tags"]["device_id"]),"timestamp":"null","value":v}
        q5_result.append(schema)
query_results.append({"query":"q_5","size_q":size_exp,"info_query":queries[4],"results":q5_result})
'''for i in q5_result:
    print(i)'''

result_query_mquery = (id_exp_query+5,db_exp,type_q[1],n_q,len(devices_ids),size_q[0],round(time_exp_ms_query,3))
#print(result_query_mquery)
insert_query_mquery = f"INSERT INTO {m_query} (id_exp,db_exp,type_q,n_q,n_var,size_q,time_exp_ms) VALUES (%s, %s, %s, %s, %s, %s, %s)"
pg_cursor.execute(insert_query_mquery,result_query_mquery)
postgresdb_conn.commit()

#print("\n************************************************************************")
# Consulta analítica, count n_q=6 ##############################################################################################
n_q = 6

query = {
    'start': '0',
    'end' : 'now',
    "queries": [
        {
        "aggregator": "sum",
        "metric": "energy",
        "downsample": "0all-count",
        "tags": {
            "device_id": "|".join(devices_ids)
        }
        }
    ]
}

ts = time.time_ns()
select_query_mquery = requests.post(url_opentsdb_query, data=json.dumps(query))
te = time.time_ns()
time_exp_ms_query = (te-ts)/1_000_000
result_query = select_query_mquery.json()
#print(json.dumps(result_query, indent=2))

q6_result = []
for r in result_query:
    for ts, v in r["dps"].items():
        schema = {"device_id":process_str_opentsdb(r["tags"]["device_id"]),"timestamp":"null","value":int(v)}
        q6_result.append(schema)
query_results.append({"query":"q_6","size_q":size_exp,"info_query":queries[5],"results":q6_result})
'''for i in q6_result:
    print(i)'''

result_query_mquery = (id_exp_query+6,db_exp,type_q[1],n_q,len(devices_ids),size_q[0],round(time_exp_ms_query,3))
#print(result_query_mquery)
insert_query_mquery = f"INSERT INTO {m_query} (id_exp,db_exp,type_q,n_q,n_var,size_q,time_exp_ms) VALUES (%s, %s, %s, %s, %s, %s, %s)"
pg_cursor.execute(insert_query_mquery,result_query_mquery)
postgresdb_conn.commit()

#print("\n************************************************************************")
# Consulta analítica, distinct n_q=7 ###########################################################################################
n_q = 7

ts = time.time_ns()
#select_query_mquery=
te = time.time_ns()
time_exp_ms_query = (te-ts)/1_000_000
#result_query=
time_exp_ms_query=0.0

#############################
# NO ES POSIBLE EN OPENTSDB #
#############################

q7_result = []
query_results.append({"query":"q_7","size_q":size_exp,"info_query":queries[6],"results":q7_result})
'''for i in q7_result:
    print(i)'''

result_query_mquery = (id_exp_query+7,db_exp,type_q[1],n_q,len(devices_ids),size_q[0],round(time_exp_ms_query,3))
#print(result_query_mquery)

insert_query_mquery = f"INSERT INTO {m_query} (id_exp,db_exp,type_q,n_q,n_var,size_q,time_exp_ms) VALUES (%s, %s, %s, %s, %s, %s, %s)"
pg_cursor.execute(insert_query_mquery,result_query_mquery)
postgresdb_conn.commit()

#print("\n************************************************************************")
# Consulta agrupación, avg horas n_q=8 #########################################################################################
n_q = 8

query = {
    'start': '0',
    'end' : 'now',
    "queries": [
        {
        "aggregator": "avg",
        "metric": "energy",
        "downsample": "1h-avg",
        "tags": {
            "device_id": "|".join(devices_ids)
        }
        }
    ]
}

ts = time.time_ns()
select_query_mquery = requests.post(url_opentsdb_query, data=json.dumps(query))
te = time.time_ns()
time_exp_ms_query = (te-ts)/1_000_000
result_query = select_query_mquery.json()
#print(json.dumps(result_query, indent=2))

q8_1_result = []
for r in result_query:
    for ts, v in r["dps"].items():
        schema = {"device_id":process_str_opentsdb(r["tags"]["device_id"]),"timestamp":epoch_s_to_datetime(ts),"value":v}
        q8_1_result.append(schema)
query_results.append({"query":"q_8_1","size_q":size_exp,"info_query":queries[7],"results":q8_1_result})
'''for i in q8_1_result:
    print(i)'''

result_query_mquery = (id_exp_query+8,db_exp,type_q[2],n_q,len(devices_ids),size_q[1],round(time_exp_ms_query,3))
#print(result_query_mquery)

insert_query_mquery = f"INSERT INTO {m_query} (id_exp,db_exp,type_q,n_q,n_var,size_q,time_exp_ms) VALUES (%s, %s, %s, %s, %s, %s, %s)"
pg_cursor.execute(insert_query_mquery,result_query_mquery)
postgresdb_conn.commit()

#print("\n************************************************************************")
# Consulta agrupación, avg semanas n_q=8 #######################################################################################

query = {
    'start': '0',
    'end' : 'now',
    "queries": [
        {
        "aggregator": "avg",
        "metric": "energy",
        "downsample": "1w-avg",
        "tags": {
            "device_id": "|".join(devices_ids)
        }
        }
    ]
}

ts = time.time_ns()
select_query_mquery = requests.post(url_opentsdb_query, data=json.dumps(query))
te = time.time_ns()
time_exp_ms_query = (te-ts)/1_000_000
result_query = select_query_mquery.json()
#print(json.dumps(result_query, indent=2))

q8_2_result = []
for r in result_query:
    for ts, v in r["dps"].items():
        schema = {"device_id":process_str_opentsdb(r["tags"]["device_id"]),"timestamp":epoch_s_to_datetime_utc(ts),"value":v}
        q8_2_result.append(schema)
query_results.append({"query":"q_8_2","size_q":size_exp,"info_query":queries[8],"results":q8_2_result})
'''for i in q8_2_result:
    print(i)'''

result_query_mquery = (id_exp_query+9,db_exp,type_q[2],n_q,len(devices_ids),size_q[3],round(time_exp_ms_query,3))
#print(result_query_mquery)

insert_query_mquery = f"INSERT INTO {m_query} (id_exp,db_exp,type_q,n_q,n_var,size_q,time_exp_ms) VALUES (%s, %s, %s, %s, %s, %s, %s)"
pg_cursor.execute(insert_query_mquery,result_query_mquery)
postgresdb_conn.commit()

#print("\n************************************************************************")
# Consulta agrupación, avg meses n_q=8 #########################################################################################

query = {
    'start': '0',
    'end' : 'now',
    "queries": [
        {
        "aggregator": "avg",
        "metric": "energy",
        "downsample": "1n-avg",
        "tags": {
            "device_id": "|".join(devices_ids)
        }
        }
    ]
}

ts = time.time_ns()
select_query_mquery = requests.post(url_opentsdb_query, data=json.dumps(query))
te = time.time_ns()
time_exp_ms_query = (te-ts)/1_000_000
result_query = select_query_mquery.json()
#print(json.dumps(result_query, indent=2))

q8_3_result = []
for r in result_query:
    for ts, v in r["dps"].items():
        schema = {"device_id":process_str_opentsdb(r["tags"]["device_id"]),"timestamp":epoch_s_to_datetime_utc(ts),"value":v}
        q8_3_result.append(schema)
query_results.append({"query":"q_8_3","size_q":size_exp,"info_query":queries[9],"results":q8_3_result})
'''for i in q8_3_result:
    print(i)'''

result_query_mquery = (id_exp_query+10,db_exp,type_q[2],n_q,len(devices_ids),size_q[4],round(time_exp_ms_query,3))
#print(result_query_mquery)

insert_query_mquery = f"INSERT INTO {m_query} (id_exp,db_exp,type_q,n_q,n_var,size_q,time_exp_ms) VALUES (%s, %s, %s, %s, %s, %s, %s)"
pg_cursor.execute(insert_query_mquery,result_query_mquery)
postgresdb_conn.commit()

#print("\n************************************************************************")
# Consulta agrupación, outliers n_q=9 ##########################################################################################
info_query_9="Obtener para un determinado sensor el tercer cuartil de sus mediciones"
n_q = 9

query = {
    'start': '0',
    'end' : 'now',
    "queries": [
        {
        "aggregator": "p75",
        "metric": "energy",
        "downsample": "0all-p75",
        "tags": {
            "device_id": "|".join(devices_ids)
        }
        }
    ]
}

ts = time.time_ns()
select_query_mquery = requests.post(url_opentsdb_query, data=json.dumps(query))
te = time.time_ns()
time_exp_ms_query = (te-ts)/1_000_000
result_query = select_query_mquery.json()
#print(json.dumps(result_query, indent=2))

q9_result = []
for r in result_query:
    for ts, v in r["dps"].items():
        schema = {"device_id":process_str_opentsdb(r["tags"]["device_id"]),"timestamp":"null","value":v}
        q9_result.append(schema)
query_results.append({"query":"q_9","size_q":size_exp,"info_query":info_query_9,"results":q9_result})
'''for i in q9_result:
    print(i)'''

result_query_mquery = (id_exp_query+11,db_exp,type_q[2],n_q,len(devices_ids),size_q[0],round(time_exp_ms_query,3))
#print(result_query_mquery)

insert_query_mquery = f"INSERT INTO {m_query} (id_exp,db_exp,type_q,n_q,n_var,size_q,time_exp_ms) VALUES (%s, %s, %s, %s, %s, %s, %s)"
pg_cursor.execute(insert_query_mquery,result_query_mquery)
postgresdb_conn.commit()

#print("\n************************************************************************")
# Consulta rangos de tiempo, mediciones 1 dia n_q=10 ###########################################################################
timestamp_tr_start = "2019-01-22T00:00:00"
timestamp_tr_end = "2019-01-23T00:00:00"
n_q = 10

query = {
    'start': datetime_to_epoch_ms(datetime.fromisoformat(timestamp_tr_start)),
    'end' : datetime_to_epoch_ms(datetime.fromisoformat(timestamp_tr_end)),
    "queries": [
        {
        "aggregator": "none",
        "metric": "energy",
        "tags": {
            "device_id": "|".join(devices_ids)
        }
        }
    ]
}

ts = time.time_ns()
select_query_mquery = requests.post(url_opentsdb_query, data=json.dumps(query))
te = time.time_ns()
time_exp_ms_query = (te-ts)/1_000_000
result_query = select_query_mquery.json()
#print(json.dumps(result_query, indent=2))

q10_1_result = []
for r in result_query:
    for ts, v in r["dps"].items():
        schema = {"device_id":process_str_opentsdb(r["tags"]["device_id"]),"timestamp":epoch_s_to_datetime(ts),"value":v}
        q10_1_result.append(schema)
query_results.append({"query":"q_10_1","size_q":size_exp,"info_query":queries[11],"results":q10_1_result})
'''for i in q10_1_result:
    print(i)'''

result_query_mquery = (id_exp_query+12,db_exp,type_q[3],n_q,len(devices_ids),size_q[2],round(time_exp_ms_query,3))
#print(result_query_mquery)
insert_query_mquery = f"INSERT INTO {m_query} (id_exp,db_exp,type_q,n_q,n_var,size_q,time_exp_ms) VALUES (%s, %s, %s, %s, %s, %s, %s)"
pg_cursor.execute(insert_query_mquery,result_query_mquery)
postgresdb_conn.commit()

#print("\n************************************************************************")
# Consulta rangos de tiempo, mediciones ++1 dia n_q=10 #########################################################################
timestamp_tr_start = "2019-04-16T00:00:00"
timestamp_tr_end = "2019-04-17T18:00:00"

query = {
    'start': datetime_to_epoch_ms(datetime.fromisoformat(timestamp_tr_start)),
    'end' : datetime_to_epoch_ms(datetime.fromisoformat(timestamp_tr_end)),
    "queries": [
        {
        "aggregator": "none",
        "metric": "energy",
        "tags": {
            "device_id": "|".join(devices_ids)
        }
        }
    ]
}

ts = time.time_ns()
select_query_mquery = requests.post(url_opentsdb_query, data=json.dumps(query))
te = time.time_ns()
time_exp_ms_query = (te-ts)/1_000_000
result_query = select_query_mquery.json()
#print(json.dumps(result_query, indent=2))

q10_2_result = []
for r in result_query:
    for ts, v in r["dps"].items():
        schema = {"device_id":process_str_opentsdb(r["tags"]["device_id"]),"timestamp":epoch_s_to_datetime(ts),"value":v}
        q10_2_result.append(schema)
query_results.append({"query":"q_10_2","size_q":size_exp,"info_query":queries[12],"results":q10_2_result})
'''for i in q10_2_result:
    print(i)'''

result_query_mquery = (id_exp_query+13,db_exp,type_q[3],n_q,len(devices_ids),size_q[5],round(time_exp_ms_query,3))
#print(result_query_mquery)
insert_query_mquery = f"INSERT INTO {m_query} (id_exp,db_exp,type_q,n_q,n_var,size_q,time_exp_ms) VALUES (%s, %s, %s, %s, %s, %s, %s)"
pg_cursor.execute(insert_query_mquery,result_query_mquery)
postgresdb_conn.commit()

#print("\n************************************************************************")
# Consulta rangos de tiempo, mediciones 1 mes n_q=10 ###########################################################################
timestamp_tr_start = "2019-01-01T00:00:00"
timestamp_tr_end = "2019-02-01T00:00:00"

query = {
    'start': datetime_to_epoch_ms(datetime.fromisoformat(timestamp_tr_start)),
    'end' : datetime_to_epoch_ms(datetime.fromisoformat(timestamp_tr_end)),
    "queries": [
        {
        "aggregator": "none",
        "metric": "energy",
        "tags": {
            "device_id": "|".join(devices_ids)
        }
        }
    ]
}

ts = time.time_ns()
select_query_mquery = requests.post(url_opentsdb_query, data=json.dumps(query))
te = time.time_ns()
time_exp_ms_query = (te-ts)/1_000_000
result_query = select_query_mquery.json()
#print(json.dumps(result_query, indent=2))

q10_3_result = []
for r in result_query:
    for ts, v in r["dps"].items():
        schema = {"device_id":process_str_opentsdb(r["tags"]["device_id"]),"timestamp":epoch_s_to_datetime(ts),"value":v}
        q10_3_result.append(schema)
query_results.append({"query":"q_10_3","size_q":size_exp,"info_query":queries[13],"results":q10_3_result})
'''for i in q10_3_result:
    print(i)'''

result_query_mquery = (id_exp_query+14,db_exp,type_q[3],n_q,len(devices_ids),size_q[4],round(time_exp_ms_query,3))
#print(result_query_mquery)
insert_query_mquery = f"INSERT INTO {m_query} (id_exp,db_exp,type_q,n_q,n_var,size_q,time_exp_ms) VALUES (%s, %s, %s, %s, %s, %s, %s)"
pg_cursor.execute(insert_query_mquery,result_query_mquery)
postgresdb_conn.commit()

#print("\n************************************************************************")
# Consulta rangos de tiempo, mediciones ++1 mes n_q=10 #########################################################################
timestamp_tr_start = "2019-01-01T00:00:00"
timestamp_tr_end = "2019-02-15T00:00:00"

query = {
    'start': datetime_to_epoch_ms(datetime.fromisoformat(timestamp_tr_start)),
    'end' : datetime_to_epoch_ms(datetime.fromisoformat(timestamp_tr_end)),
    "queries": [
        {
        "aggregator": "none",
        "metric": "energy",
        "tags": {
            "device_id": "|".join(devices_ids)
        }
        }
    ]
}

ts = time.time_ns()
select_query_mquery = requests.post(url_opentsdb_query, data=json.dumps(query))
te = time.time_ns()
time_exp_ms_query = (te-ts)/1_000_000
result_query = select_query_mquery.json()
#print(json.dumps(result_query, indent=2))

q10_4_result = []
for r in result_query:
    for ts, v in r["dps"].items():
        schema = {"device_id":process_str_opentsdb(r["tags"]["device_id"]),"timestamp":epoch_s_to_datetime(ts),"value":v}
        q10_4_result.append(schema)
query_results.append({"query":"q_10_4","size_q":size_exp,"info_query":queries[14],"results":q10_4_result})
'''for i in q10_4_result:
    print(i)'''

result_query_mquery = (id_exp_query+15,db_exp,type_q[3],n_q,len(devices_ids),size_q[6],round(time_exp_ms_query,3))
#print(result_query_mquery)
insert_query_mquery = f"INSERT INTO {m_query} (id_exp,db_exp,type_q,n_q,n_var,size_q,time_exp_ms) VALUES (%s, %s, %s, %s, %s, %s, %s)"
pg_cursor.execute(insert_query_mquery,result_query_mquery)
postgresdb_conn.commit()

#print("\n************************************************************************")
##########################################################################################################################################################
##########################################################################################################################################################
# Consultas tamaño de almacenamiento

insert_query_mspace = "INSERT INTO m_space (id_exp,db_exp,size_exp,space_exp_mb) VALUES (%s, %s, %s, %s)"

# Conseguir el tamaño del archivo json en MB
space_exp_json = os.path.getsize(dir_data_path)/1_048_576

# Almacenar resultado
result_query_mspace1 = (id_exp_space+1,'jsonopen',size_exp,round(space_exp_json,3))
#print("\n************************************************************************")
#print("\ntables ---> m_space")
#print(result_query_mspace1)
pg_cursor.execute(insert_query_mspace,result_query_mspace1)
postgresdb_conn.commit()

# Conseguir tamaño de almacenamiento en disco de opentsdb en MB
cmd = "du -s /data/hbase"

# Cliente Docker
client_docker = docker.DockerClient(base_url='unix://var/run/docker.sock')
try:
    # Ejecuta el comando Bash en el contenedor
    container_open = client_docker.containers.get(db_container_opentsdb)
    result_cmd = container_open.exec_run(cmd)
    space_size_open = process_space_db(result_cmd.output.decode('utf-8'))
    result_query_mspace2 = (id_exp_space+2,db_exp,size_exp,round(space_size_open,3))
    
except docker.errors.APIError as e:
    result_query_mspace2 = (id_exp_space+2,db_exp,size_exp,0.0)
    
finally:
    pg_cursor.execute(insert_query_mspace,result_query_mspace2)
    postgresdb_conn.commit()
    client_docker.close()

##########################################################################################################################################################
##########################################################################################################################################################
# Resultados

export_results_tojson(query_results,db_exp,str(size_exp),str(len(devices_ids)))

##########################################################################################################################################################
# Cerrar cursores y conexiones

pg_cursor.close()
postgresdb_conn.close()

print("\n#############\nFin de pruebas en Opentsdb\n#############")
##########################################################################################################################################################
