import psycopg2
from influxdb_client import InfluxDBClient, WriteOptions
from influxdb_client.client.write_api import SYNCHRONOUS
import json, os, re, time, sys
from datetime import datetime
import docker

#############################################################################
# Contenedor InfluxDB
db_container_influx="influx_db_container"

# InfluxDB
bucket = "mybucket"
org = "myorg"
token = "mytoken"
url="http://localhost:8086"
username="admin"
password="admin123456789"

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
# Consultas

list_device = ["AInt'PS'AnlzRed21'En1",
"AInt'PS'AnlzRed41'En1",
"AInt'PS'AnlzRed29'AI65",
"AInt'P1'AnlzRed01'AI65",
"AInt'P2'AnlzRed10'AI65",
"AInt'PS'AnlzRed28'AI65",
"AInt'PS'AnlzRed35'AI65",
"AInt'PS'AnlzRed39'En1",
"AInt'PB'AnlzRed07'AI65",
"AInt'PB'AnlzRed13'AI65",
"AInt'PB'AnlzRed14'AI65",
"AInt'P1'AnlzRed07'AI65",
"AInt'P1'AnlzRed11'AI65",
"AInt'P1'AnlzRed24'AI65",
"AInt'P1'AnlzRed25'AI65",
"AInt'P2'AnlzRed05'AI65",
"AInt'P2'AnlzRed06'AI65",
"AInt'P2'AnlzRed26'AI65",
"AInt'P2'AnlzRed27'AI65",
"AInt'PS'AnlzRed23'PotT"]

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

# Procesa la salida del comando pasado al contenedor de docker
def process_space_db (str_space):
    #Expresion regular para el tamaño de los datos en influxdb
    r_space = re.compile('\d+(\.\d+)?')
    str_space = r_space.search(str_space).group(0)
    # Convertir cantidad de B a MB
    space = float(str_space)/1024

    return space

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
dir_json_path="./data/json_schemas/influxdb"

# Conexiones

# PostgresDB
postgresdb_conn = psycopg2.connect(
    host=db_host_psql,
    port=db_port_psql,
    user=db_user_psql,
    password=db_pssw_psql,
    database=db_name_psql
)

# InfluxDB
client = InfluxDBClient(
    timeout=50000000,
    url=url,
    token=token,
    org=org,
    username=username,
    password=password
)
pg_cursor = postgresdb_conn.cursor()

write_api = client.write_api(write_options=SYNCHRONOUS)
query_api = client.query_api()
delete_api = client.delete_api()

##########################################################################################################################################################
##########################################################################################################################################################
# Índices

pg_cursor.execute("SELECT MAX(id_exp) FROM m_data WHERE db_exp = 'influxdb'")
id_exp_data = pg_cursor.fetchall()[0][0]
id_exp_data = 0 if id_exp_data == None else int(id_exp_data)

pg_cursor.execute("SELECT MAX(id_exp) FROM m_space WHERE db_exp = 'influxdb'")
id_exp_space = pg_cursor.fetchall()[0][0]
id_exp_space = 0 if id_exp_space == None else int(id_exp_space)

pg_cursor.execute("SELECT MAX(id_exp) FROM m_query_3 WHERE db_exp = 'influxdb'")
id_exp_query_3 = pg_cursor.fetchall()[0][0]
id_exp_query_3 = 0 if id_exp_query_3 == None else int(id_exp_query_3)

pg_cursor.execute("SELECT MAX(id_exp) FROM m_query_6 WHERE db_exp = 'influxdb'")
id_exp_query_6 = pg_cursor.fetchall()[0][0]
id_exp_query_6 = 0 if id_exp_query_6 == None else int(id_exp_query_6)

pg_cursor.execute("SELECT MAX(id_exp) FROM m_query_10 WHERE db_exp = 'influxdb'")
id_exp_query_10 = pg_cursor.fetchall()[0][0]
id_exp_query_10 = 0 if id_exp_query_10 == None else int(id_exp_query_10)

pg_cursor.execute("SELECT MAX(id_exp) FROM m_query_13 WHERE db_exp = 'influxdb'")
id_exp_query_13 = pg_cursor.fetchall()[0][0]
id_exp_query_13 = 0 if id_exp_query_13 == None else int(id_exp_query_13) 


db_exp = "influxdb"
size_exp = int(sys.argv[-1])

if size_exp == 3:
    dir_data_path = dir_json_path + "/influxdb_3.json"
    m_query = "m_query_3"
    id_exp_query = id_exp_query_3
elif size_exp == 6:
    dir_data_path = dir_json_path + "/influxdb_6.json"
    m_query = "m_query_6"
    id_exp_query = id_exp_query_6
elif size_exp == 10:
    dir_data_path = dir_json_path + "/influxdb_10.json"
    m_query = "m_query_10"
    id_exp_query = id_exp_query_10
else:
    dir_data_path = dir_json_path + "/influxdb_13.json"
    m_query = "m_query_13"
    id_exp_query = id_exp_query_13

# Consultas carga de datos
with open(dir_data_path, "r") as json_file:
    data = json.load(json_file)

ts = time.time_ns()
write_api.write(bucket=bucket, org=org, record=data)
te = time.time_ns()
time_exp_ms_data = (te-ts)/1_000_000
#print(round(time_exp_ms_data,3))

# Almacenar resultado
result_query_mdata = (id_exp_data+1,db_exp,size_exp,round(time_exp_ms_data,3))
#print("\n************************************************************************")
#print("tables ---> m_data")
#print(result_query_mdata)

insert_query_mdata = "INSERT INTO m_data (id_exp,db_exp,size_exp,time_exp_ms) VALUES (%s, %s, %s, %s)"
pg_cursor.execute(insert_query_mdata,result_query_mdata)
postgresdb_conn.commit()

##########################################################################################################################################################
##########################################################################################################################################################
# Consultas latencia de consultas

#print("\n************************************************************************")
#print("\ntables ---> m_query_*")

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
timestamp_ep_start = "1970-01-01T00:00:00Z"
timestamp_ep = "2019-03-02T10:21:20Z"
n_q = 1

records_template = " or ".join([f'r.device_id == "{dev}"' for dev in devices_ids])

query = '''from(bucket: "mybucket")
    |> range(start: time(v: {}), stop: now())
    |> filter(fn: (r) => r._measurement == "energy" and ({}))
    |> filter(fn: (r) => r._time == time(v: {}))'''.format(timestamp_ep_start,records_template,timestamp_ep)

ts = time.time_ns()
select_query_mquery = query_api.query(query)
te = time.time_ns()
time_exp_ms_query = (te-ts)/1_000_000

result_query_select = select_query_mquery.to_values(columns=['device_id', '_time', '_value'])
q1_result = []
for r in result_query_select:
    schema = {"device_id":r[0],"timestamp":r[1].strftime("%Y-%m-%d %H:%M:%S"),"value":r[2]}
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
timestamp_start = "1970-01-01T00:00:00Z"
n_q = 2

records_template = " or ".join([f'r.device_id == "{dev}"' for dev in devices_ids])

query = '''from(bucket: "mybucket")
    |> range(start: time(v: {}), stop: now())
    |> filter(fn: (r) => r._measurement == "energy" and ({}))
    |> group(columns: ["device_id"])
    |> min(column: "_value")
    |> sort(columns: ["device_id"])'''.format(timestamp_start,records_template)

ts = time.time_ns()
select_query_mquery = query_api.query(query)
te = time.time_ns()
time_exp_ms_query = (te-ts)/1_000_000

result_query_select = select_query_mquery.to_values(columns=['device_id','_value'])
q2_result = []
for r in result_query_select:
    schema = {"device_id":r[0],"timestamp":"null","value":r[1]}
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
timestamp_start = "1970-01-01T00:00:00Z"
n_q = 3

records_template = " or ".join([f'r.device_id == "{dev}"' for dev in devices_ids])

query = '''from(bucket: "mybucket")
    |> range(start: time(v: {}), stop: now())
    |> filter(fn: (r) => r._measurement == "energy" and ({}))
    |> group(columns: ["device_id"])
    |> max(column: "_value")
    |> sort(columns: ["device_id"])'''.format(timestamp_start,records_template)

ts = time.time_ns()
select_query_mquery = query_api.query(query)
te = time.time_ns()
time_exp_ms_query = (te-ts)/1_000_000

result_query_select = select_query_mquery.to_values(columns=['device_id','_value'])
q3_result = []
for r in result_query_select:
    schema = {"device_id":r[0],"timestamp":"null","value":r[1]}
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
timestamp_start = "1970-01-01T00:00:00Z"
n_q = 4

records_template = " or ".join([f'r.device_id == "{dev}"' for dev in devices_ids])

query = '''from(bucket: "mybucket")
    |> range(start: time(v: {}), stop: now())
    |> filter(fn: (r) => r._measurement == "energy" and ({}))
    |> group(columns: ["device_id"])
    |> mean()
    |> sort(columns: ["device_id"])'''.format(timestamp_start,records_template)

ts = time.time_ns()
select_query_mquery = query_api.query(query)
te = time.time_ns()
time_exp_ms_query = (te-ts)/1_000_000

result_query_select = select_query_mquery.to_values(columns=['device_id','_value'])
q4_result = []
for r in result_query_select:
    schema = {"device_id":r[0],"timestamp":"null","value":r[1]}
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
timestamp_start = "1970-01-01T00:00:00Z"
n_q = 5

records_template = " or ".join([f'r.device_id == "{dev}"' for dev in devices_ids])

query = '''from(bucket: "mybucket")
    |> range(start: time(v: {}), stop: now())
    |> filter(fn: (r) => r._measurement == "energy" and ({}))
    |> group(columns: ["device_id"])
    |> stddev()
    |> sort(columns: ["device_id"])'''.format(timestamp_start,records_template)

ts = time.time_ns()
select_query_mquery = query_api.query(query)
te = time.time_ns()
time_exp_ms_query = (te-ts)/1_000_000

result_query_select = select_query_mquery.to_values(columns=['device_id','_value'])
q5_result = []
for r in result_query_select:
    schema = {"device_id":r[0],"timestamp":"null","value":r[1]}
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
timestamp_start = "1970-01-01T00:00:00Z"
n_q = 6

records_template = " or ".join([f'r.device_id == "{dev}"' for dev in devices_ids])

query = '''from(bucket: "mybucket")
    |> range(start: time(v: {}), stop: now())
    |> filter(fn: (r) => r._measurement == "energy" and ({}))
    |> group(columns: ["device_id"])
    |> count()
    |> sort(columns: ["device_id"])'''.format(timestamp_start,records_template)

ts = time.time_ns()
select_query_mquery = query_api.query(query)
te = time.time_ns()
time_exp_ms_query = (te-ts)/1_000_000

result_query_select = select_query_mquery.to_values(columns=['device_id','_value'])
q6_result = []
for r in result_query_select:
    schema = {"device_id":r[0],"timestamp":"null","value":r[1]}
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
timestamp_start = "1970-01-01T00:00:00Z"
n_q = 7

records_template = " or ".join([f'r.device_id == "{dev}"' for dev in devices_ids])

query = '''from(bucket: "mybucket")
    |> range(start: time(v: {}), stop: now())
    |> filter(fn: (r) => r._measurement == "energy" and ({}))
    |> group(columns: ["device_id"])
    |> distinct(column: "_value")
    |> sort(columns: ["device_id","_time"])'''.format(timestamp_start,records_template)

ts = time.time_ns()
select_query_mquery = query_api.query(query)
te = time.time_ns()
time_exp_ms_query = (te-ts)/1_000_000

result_query_select = select_query_mquery.to_values(columns=['device_id','_value'])
q7_result = []
for r in result_query_select:
    schema = {"device_id":r[0],"timestamp":"null","value":r[1]}
    q7_result.append(schema)
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
timestamp_start = "1970-01-01T00:00:00Z"
n_q = 8

records_template = " or ".join([f'r.device_id == "{dev}"' for dev in devices_ids])

query = '''from(bucket: "mybucket")
    |> range(start: time(v: {}), stop: now())
    |> filter(fn: (r) => r._measurement == "energy" and ({}))
    |> group(columns: ["device_id"])
    |> window(every: 1h)
    |> mean()
    |> sort(columns: ["device_id","_time"])'''.format(timestamp_start,records_template)

ts = time.time_ns()
select_query_mquery = query_api.query(query)
te = time.time_ns()
time_exp_ms_query = (te-ts)/1_000_000

result_query_select = select_query_mquery.to_values(columns=['device_id','_start','_value'])
q8_1_result = []
for r in result_query_select:
    schema = {"device_id":r[0],"timestamp":r[1].strftime("%Y-%m-%d %H:%M:%S"),"value":r[2]}
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
timestamp_start = "1970-01-01T00:00:00Z"

records_template = " or ".join([f'r.device_id == "{dev}"' for dev in devices_ids])

query = '''from(bucket: "mybucket")
    |> range(start: time(v: {}), stop: now())
    |> filter(fn: (r) => r._measurement == "energy" and ({}))
    |> group(columns: ["device_id"])
    |> window(every: 1w, offset: -3d)
    |> mean()
    |> sort(columns: ["device_id","_time"])'''.format(timestamp_start,records_template)

ts = time.time_ns()
select_query_mquery = query_api.query(query)
te = time.time_ns()
time_exp_ms_query = (te-ts)/1_000_000

result_query_select = select_query_mquery.to_values(columns=['device_id','_start','_value'])
q8_2_result = []
for r in result_query_select:
    schema = {"device_id":r[0],"timestamp":r[1].strftime("%Y-%m-%d %H:%M:%S"),"value":r[2]}
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
timestamp_start = "1970-01-01T00:00:00Z"

records_template = " or ".join([f'r.device_id == "{dev}"' for dev in devices_ids])

query = '''from(bucket: "mybucket")
    |> range(start: time(v: {}), stop: now())
    |> filter(fn: (r) => r._measurement == "energy" and ({}))
    |> group(columns: ["device_id"])
    |> window(every: 1mo)
    |> mean()
    |> sort(columns: ["device_id","_time"])'''.format(timestamp_start,records_template)

ts = time.time_ns()
select_query_mquery = query_api.query(query)
te = time.time_ns()
time_exp_ms_query = (te-ts)/1_000_000

result_query_select = select_query_mquery.to_values(columns=['device_id','_start','_value'])
q8_3_result = []
for r in result_query_select:
    schema = {"device_id":r[0],"timestamp":r[1].strftime("%Y-%m-%d %H:%M:%S"),"value":r[2]}
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
timestamp_start = "1970-01-01T00:00:00Z"
n_q = 9

records_template = " or ".join([f'r.device_id == "{dev}"' for dev in devices_ids])

query= '''from(bucket: "mybucket")
    |> range(start: time(v: {}), stop: now())
    |> filter(fn: (r) => r._measurement == "energy" and ({}))
    |> group(columns: ["device_id"])
    |> quantile(column: "_value", q: 0.75)'''.format(timestamp_start,records_template)

ts = time.time_ns()
select_query_mquery = query_api.query(query)
te = time.time_ns()
time_exp_ms_query = (te-ts)/1_000_000

result_query_select = select_query_mquery.to_values(columns=['device_id','_value'])
q9_result = []
for r in result_query_select:
    schema = {"device_id":r[0],"timestamp":"null","value":r[1]}
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
timestamp_tr_start = "2019-01-22T00:00:00Z"
timestamp_tr_end = "2019-01-23T00:00:00Z"
n_q = 10

records_template = " or ".join([f'r.device_id == "{dev}"' for dev in devices_ids])

query = '''from(bucket: "mybucket")
    |> range(start: time(v: {}), stop: time(v: {}))
    |> filter(fn: (r) => r._measurement == "energy" and ({}))
    |> group(columns: ["device_id","_time"])'''.format(timestamp_tr_start,timestamp_tr_end,records_template)

ts = time.time_ns()
select_query_mquery = query_api.query(query)
te = time.time_ns()
time_exp_ms_query = (te-ts)/1_000_000

result_query_select = select_query_mquery.to_values(columns=['device_id','_time','_value'])
q10_1_result = []
for r in result_query_select:
    schema = {"device_id":r[0],"timestamp":r[1].strftime("%Y-%m-%d %H:%M:%S"),"value":r[2]}
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
timestamp_tr_start = "2019-04-16T00:00:00Z"
timestamp_tr_end = "2019-04-17T18:00:00Z"

records_template = " or ".join([f'r.device_id == "{dev}"' for dev in devices_ids])

query = '''from(bucket: "mybucket")
    |> range(start: time(v: {}), stop: time(v: {}))
    |> filter(fn: (r) => r._measurement == "energy" and ({}))
    |> group(columns: ["device_id","_time"])'''.format(timestamp_tr_start,timestamp_tr_end,records_template)

ts = time.time_ns()
select_query_mquery = query_api.query(query)
te = time.time_ns()
time_exp_ms_query = (te-ts)/1_000_000

result_query_select = select_query_mquery.to_values(columns=['device_id','_time','_value'])
q10_2_result = []
for r in result_query_select:
    schema = {"device_id":r[0],"timestamp":r[1].strftime("%Y-%m-%d %H:%M:%S"),"value":r[2]}
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
timestamp_tr_start = "2019-01-01T00:00:00Z"
timestamp_tr_end = "2019-02-01T00:00:00Z"

records_template = " or ".join([f'r.device_id == "{dev}"' for dev in devices_ids])

query = '''from(bucket: "mybucket")
    |> range(start: time(v: {}), stop: time(v: {}))
    |> filter(fn: (r) => r._measurement == "energy" and ({}))
    |> group(columns: ["device_id","_time"])'''.format(timestamp_tr_start,timestamp_tr_end,records_template)

ts = time.time_ns()
select_query_mquery = query_api.query(query)
te = time.time_ns()
time_exp_ms_query = (te-ts)/1_000_000

result_query_select = select_query_mquery.to_values(columns=['device_id','_time','_value'])
q10_3_result = []
for r in result_query_select:
    schema = {"device_id":r[0],"timestamp":r[1].strftime("%Y-%m-%d %H:%M:%S"),"value":r[2]}
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
timestamp_tr_start = "2019-01-01T00:00:00Z"
timestamp_tr_end = "2019-02-15T00:00:00Z"

records_template = " or ".join([f'r.device_id == "{dev}"' for dev in devices_ids])

query = '''from(bucket: "mybucket")
    |> range(start: time(v: {}), stop: time(v: {}))
    |> filter(fn: (r) => r._measurement == "energy" and ({}))
    |> group(columns: ["device_id","_time"])'''.format(timestamp_tr_start,timestamp_tr_end,records_template)

ts = time.time_ns()
select_query_mquery = query_api.query(query)
te = time.time_ns()
time_exp_ms_query = (te-ts)/1_000_000

result_query_select = select_query_mquery.to_values(columns=['device_id','_time','_value'])
q10_4_result = []
for r in result_query_select:
    schema = {"device_id":r[0],"timestamp":r[1].strftime("%Y-%m-%d %H:%M:%S"),"value":r[2]}
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
result_query_mspace1 = (id_exp_space+1,'jsoninflux',size_exp,round(space_exp_json,3))
#print("\n************************************************************************")
#print("\ntables ---> m_space")
#print(result_query_mspace1)
pg_cursor.execute(insert_query_mspace,result_query_mspace1)
postgresdb_conn.commit()

# Conseguir tamaño de almacenamiento en disco de influxdb en MB
cmd = "du -s /var/lib/influxdb2/engine"

# Cliente Docker
client_docker = docker.DockerClient(base_url='unix://var/run/docker.sock')
try:
    # Ejecuta el comando Bash en el contenedor
    container_influx = client_docker.containers.get(db_container_influx)
    result_cmd = container_influx.exec_run(cmd)
    space_size_influx = process_space_db(result_cmd.output.decode('utf-8'))
    result_query_mspace2 = (id_exp_space+2,db_exp,size_exp,round(space_size_influx,3))
    #print(result_query_mspace2)
    
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

client.close()
pg_cursor.close()
postgresdb_conn.close()

print("\n#############\nFin de pruebas en InfluxDB\n#############")
##########################################################################################################################################################
