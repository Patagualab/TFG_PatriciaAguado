import psycopg2
import json, os, re, time, sys

#############################################################################
# Contenedor TimescaleDB 
db_container_tsql="timescale_db_container"

# TimescaleDB
db_host_tsql="localhost"
db_port_tsql="5434"
db_name_tsql="timescale_db"
db_user_tsql="admin"
db_pssw_tsql="admin"

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
dir_json_path="./data/json_schemas/timescaledb"

# Conexiones

# TimescaleDB
timescaledb_conn = psycopg2.connect(
    host=db_host_tsql,
    port=db_port_tsql,
    user=db_user_tsql,
    password=db_pssw_tsql,
    database=db_name_tsql
)
ts_cursor = timescaledb_conn.cursor()

# PostgresDB
postgresdb_conn = psycopg2.connect(
    host=db_host_psql,
    port=db_port_psql,
    user=db_user_psql,
    password=db_pssw_psql,
    database=db_name_psql
)
pg_cursor = postgresdb_conn.cursor()

##########################################################################################################################################################
##########################################################################################################################################################
# Índices

pg_cursor.execute("SELECT MAX(id_exp) FROM m_data WHERE db_exp = 'timescaledb'")
id_exp_data = pg_cursor.fetchall()[0][0]
id_exp_data = 0 if id_exp_data == None else int(id_exp_data)

pg_cursor.execute("SELECT MAX(id_exp) FROM m_space WHERE db_exp = 'timescaledb'")
id_exp_space = pg_cursor.fetchall()[0][0]
id_exp_space = 0 if id_exp_space == None else int(id_exp_space)

pg_cursor.execute("SELECT MAX(id_exp) FROM m_query_3 WHERE db_exp = 'timescaledb'")
id_exp_query_3 = pg_cursor.fetchall()[0][0]
id_exp_query_3 = 0 if id_exp_query_3 == None else int(id_exp_query_3)

pg_cursor.execute("SELECT MAX(id_exp) FROM m_query_6 WHERE db_exp = 'timescaledb'")
id_exp_query_6 = pg_cursor.fetchall()[0][0]
id_exp_query_6 = 0 if id_exp_query_6 == None else int(id_exp_query_6)

pg_cursor.execute("SELECT MAX(id_exp) FROM m_query_10 WHERE db_exp = 'timescaledb'")
id_exp_query_10 = pg_cursor.fetchall()[0][0]
id_exp_query_10 = 0 if id_exp_query_10 == None else int(id_exp_query_10)

pg_cursor.execute("SELECT MAX(id_exp) FROM m_query_13 WHERE db_exp = 'timescaledb'")
id_exp_query_13 = pg_cursor.fetchall()[0][0]
id_exp_query_13 = 0 if id_exp_query_13 == None else int(id_exp_query_13)

db_exp = "timescaledb"
size_exp = int(sys.argv[-1])

if size_exp == 3:
    dir_data_path = dir_json_path + "/timescaledb_3.json"
    size_batch = 10_000
    m_query = "m_query_3"
    id_exp_query = id_exp_query_3
elif size_exp == 6:
    dir_data_path = dir_json_path + "/timescaledb_6.json"
    size_batch = 20_000
    m_query = "m_query_6"
    id_exp_query = id_exp_query_6
elif size_exp == 10:
    dir_data_path = dir_json_path + "/timescaledb_10.json"
    size_batch = 50_000
    m_query = "m_query_10"
    id_exp_query = id_exp_query_10
else:
    dir_data_path = dir_json_path + "/timescaledb_13.json"
    size_batch = 50_000
    m_query = "m_query_13"
    id_exp_query = id_exp_query_13

# Consultas carga de datos
with open(dir_data_path, "r") as json_file:
    data = json.load(json_file)

unique_device_ids = []
cont = 0
devices_data = []

batch_t1 = []
batch_t2 = []

time_exp_ms_data_total = 0.0

for record in data:
    # Para la tabla de dispositivos solo queremos insertar una vez cada identificador porque es claver primaria
    # Asique les procesaremos aquí para obtener todos
    if record["device_id"] not in unique_device_ids:
        unique_device_ids.append(record["device_id"])
        devices_data.append((record["device_id"],record["unit_measurement"],record["information"]))

    batch_t2.append((record["device_id"],record["timestamp"],record["device_measurement"],record["type_of_tag"]))
    
    records_template = ",".join(["%s"]*size_batch)
    
    if len(batch_t2) == size_batch:
        # Inserción de lote en energy
        insert_query_t2 =  "INSERT INTO energy (device_id,timestamp,device_measurement,type_of_tag) VALUES {}".format(records_template)
        ts = time.time_ns()
        ts_cursor.execute(insert_query_t2,batch_t2)
        te = time.time_ns()

        time_exp_ms_data = (te-ts)/1_000_000
        time_exp_ms_data_total += time_exp_ms_data
        timescaledb_conn.commit()

        batch_t2 = []

    elif batch_t2 and cont == len(data)-1:
        records_template = ",".join(["%s"]*len(batch_t2))
        # Inserción restante si no se alcanza un lote en energy
        insert_query_t2 =  "INSERT INTO energy (device_id,timestamp,device_measurement,type_of_tag) VALUES {}".format(records_template)
        ts = time.time_ns()
        ts_cursor.execute(insert_query_t2,batch_t2)
        te = time.time_ns()

        time_exp_ms_data = (te-ts)/1_000_000
        time_exp_ms_data_total += time_exp_ms_data
        timescaledb_conn.commit()

        batch_t2 = []

    cont += 1

# Una vez se han procesado todos los identificadores únicos que hay en el conjunto de datos se insertan por lotes
for dev in range(len(devices_data)):
    batch_t1.append(devices_data[dev])
    records_template = ",".join(["%s"]*size_batch)

    if len(batch_t1) == size_batch:
        # Inserción de lote en devices
        insert_query_t1 =  "INSERT INTO devices (device_id,unit_measurement,information) VALUES {}".format(records_template)
        ts = time.time_ns()
        ts_cursor.execute(insert_query_t1,batch_t1)
        te = time.time_ns()

        time_exp_ms_data = (te-ts)/1_000_000
        time_exp_ms_data_total += time_exp_ms_data
        timescaledb_conn.commit()

        batch_t1 = []

    elif batch_t1 and dev == len(devices_data)-1:
        records_template = ",".join(["%s"]*len(batch_t1))
        # Inserción restante si no se alcanza un lote en devices
        insert_query_t1 =  "INSERT INTO devices (device_id,unit_measurement,information) VALUES {}".format(records_template)
        ts = time.time_ns()
        ts_cursor.execute(insert_query_t1,batch_t1)
        te = time.time_ns()

        time_exp_ms_data = (te-ts)/1_000_000
        time_exp_ms_data_total += time_exp_ms_data
        timescaledb_conn.commit()

        batch_t1 = []

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
# Consultas tamaño de almacenamiento

insert_query_mspace = "INSERT INTO m_space (id_exp,db_exp,size_exp,space_exp_mb) VALUES (%s, %s, %s, %s)"

# Conseguir el tamaño del archivo json en MB
space_exp_json = os.path.getsize(dir_data_path)/1_048_576

# Almacenar resultado
result_query_mspace1 = (id_exp_space+1,'jsontimescale',size_exp,round(space_exp_json,3))

#print("\n************************************************************************")
#print("\ntables ---> m_space")
#print(result_query_mspace1)
pg_cursor.execute(insert_query_mspace,result_query_mspace1)
postgresdb_conn.commit()

# Conseguir tamaño de almacenamiento en disco por tablas de timescaledb incluyendo datos e índices en MB
ts_cursor.execute("SELECT pg_database_size('timescale_db')")
space_exp_ts = ts_cursor.fetchall()[0][0]/1_048_576

result_query_mspace2 = (id_exp_space+2,db_exp,size_exp,round(space_exp_ts,3))
#print(result_query_mspace2)
pg_cursor.execute(insert_query_mspace,result_query_mspace2)
postgresdb_conn.commit()

##########################################################################################################################################################
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

records_template = ",".join(["%s"]*len(devices_ids))
type_q = ["ep","agg","gb","tr"]
size_q = ["all","hour","day","week","month","dayhour","monthweek"]

query_results = []

# Consulta instante de tiempo, punto exacto, n_q=1 #############################################################################
timestamp_ep = "'2019-03-02T10:21:20'"
n_q = 1

select_query_mquery = "SELECT device_id, timestamp, device_measurement FROM energy WHERE device_id IN ({}) AND timestamp={}".format(records_template,timestamp_ep)

ts = time.time_ns()
ts_cursor.execute(select_query_mquery,devices_ids)
te = time.time_ns()
time_exp_ms_query = (te-ts)/1_000_000

result_query_select = ts_cursor.fetchall()
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
n_q = 2

select_query_mquery = "SELECT device_id, MIN(device_measurement) AS min_energy FROM energy WHERE device_id IN ({}) GROUP BY device_id ORDER BY device_id".format(records_template)

ts = time.time_ns()
ts_cursor.execute(select_query_mquery,devices_ids)
te = time.time_ns()
time_exp_ms_query = (te-ts)/1_000_000

result_query_select = ts_cursor.fetchall()
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
n_q = 3

select_query_mquery = "SELECT device_id, MAX(device_measurement) AS max_energy FROM energy WHERE device_id IN ({}) GROUP BY device_id ORDER BY device_id".format(records_template)

ts = time.time_ns()
ts_cursor.execute(select_query_mquery,devices_ids)
te = time.time_ns()
time_exp_ms_query = (te-ts)/1_000_000

result_query_select = ts_cursor.fetchall()
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
n_q = 4

select_query_mquery = "SELECT device_id, AVG(device_measurement) AS avg_energy FROM energy WHERE device_id IN ({}) GROUP BY device_id ORDER BY device_id".format(records_template)

ts = time.time_ns()
ts_cursor.execute(select_query_mquery,devices_ids)
te = time.time_ns()
time_exp_ms_query = (te-ts)/1_000_000

result_query_select = ts_cursor.fetchall()
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
n_q = 5

select_query_mquery = "SELECT device_id, STDDEV(device_measurement) AS stddev_energy FROM energy WHERE device_id IN ({}) GROUP BY device_id ORDER BY device_id".format(records_template)

ts = time.time_ns()
ts_cursor.execute(select_query_mquery,devices_ids)
te = time.time_ns()
time_exp_ms_query = (te-ts)/1_000_000

result_query_select = ts_cursor.fetchall()
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
n_q = 6

select_query_mquery = "SELECT device_id, COUNT(*) AS count_records FROM energy WHERE device_id IN ({}) GROUP BY device_id ORDER BY device_id".format(records_template)

ts = time.time_ns()
ts_cursor.execute(select_query_mquery,devices_ids)
te = time.time_ns()
time_exp_ms_query = (te-ts)/1_000_000  

result_query_select = ts_cursor.fetchall()
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
n_q = 7

select_query_mquery = "SELECT device_id, ARRAY_AGG(DISTINCT device_measurement) AS distinct_measurements FROM energy WHERE device_id IN ({}) GROUP BY device_id ORDER BY device_id".format(records_template)

ts = time.time_ns()
ts_cursor.execute(select_query_mquery,devices_ids)
te = time.time_ns()
time_exp_ms_query = (te-ts)/1_000_000

result_query_select = ts_cursor.fetchall()
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
n_q = 8

select_query_mquery = "SELECT device_id, AVG(device_measurement) AS avg_energy_hour, time_bucket('1 hour', timestamp) AS hour FROM energy WHERE device_id IN ({}) GROUP BY device_id, hour ORDER BY device_id, hour".format(records_template)

ts = time.time_ns()
ts_cursor.execute(select_query_mquery,devices_ids)
te = time.time_ns()
time_exp_ms_query = (te-ts)/1_000_000

result_query_select = ts_cursor.fetchall()
q8_1_result = []
for r in result_query_select:
    schema = {"device_id":r[0],"timestamp":r[2].strftime("%Y-%m-%d %H:%M:%S"),"value":r[1]}
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
select_query_mquery = "SELECT device_id, AVG(device_measurement) AS avg_energy_week, time_bucket('1 week', timestamp,'1 week'::interval) AS week FROM energy WHERE device_id IN ({}) GROUP BY device_id, week ORDER BY device_id, week".format(records_template)

ts = time.time_ns()
ts_cursor.execute(select_query_mquery,devices_ids)
te = time.time_ns()
time_exp_ms_query = (te-ts)/1_000_000

result_query_select = ts_cursor.fetchall()
q8_2_result = []
for r in result_query_select:
    schema = {"device_id":r[0],"timestamp":r[2].strftime("%Y-%m-%d %H:%M:%S"),"value":r[1]}
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

select_query_mquery = "SELECT device_id, AVG(device_measurement) AS avg_energy_month, time_bucket('1 month', timestamp) AS month FROM energy WHERE device_id IN ({}) GROUP BY device_id, month ORDER BY device_id, month".format(records_template)

ts = time.time_ns()
ts_cursor.execute(select_query_mquery,devices_ids)
te = time.time_ns()
time_exp_ms_query = (te-ts)/1_000_000

result_query_select = ts_cursor.fetchall()
q8_3_result = []
for r in result_query_select:
    schema = {"device_id":r[0],"timestamp":r[2].strftime("%Y-%m-%d %H:%M:%S"),"value":r[1]}
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
n_q = 9

select_query_mquery = '''WITH temporal_data AS (SELECT device_id,
PERCENTILE_CONT(0.25) WITHIN GROUP (ORDER BY device_measurement) AS q1,
PERCENTILE_CONT(0.75) WITHIN GROUP (ORDER BY device_measurement) AS q3
FROM energy
WHERE device_id IN ({}) GROUP BY device_id)

SELECT energy.device_id, energy.timestamp, energy.device_measurement
FROM energy INNER JOIN temporal_data ON energy.device_id = temporal_data.device_id
WHERE energy.device_measurement < (temporal_data.q1-(1.5*(temporal_data.q3-temporal_data.q1)))
OR energy.device_measurement > (temporal_data.q3+(1.5*(temporal_data.q3-temporal_data.q1)))'''.format(records_template)

ts = time.time_ns()
ts_cursor.execute(select_query_mquery,devices_ids)
te = time.time_ns()
time_exp_ms_query = (te-ts)/1_000_000

result_query_select = ts_cursor.fetchall()
q9_result = []
for r in result_query_select:
    schema = {"device_id":r[0],"timestamp":r[1].strftime("%Y-%m-%d %H:%M:%S"),"value":r[2]}
    q9_result.append(schema)
query_results.append({"query":"q_9","size_q":size_exp,"info_query":queries[10],"results":q9_result})
'''for i in q9_result:
    print(i)'''

result_query_mquery = (id_exp_query+11,db_exp,type_q[2],n_q,len(devices_ids),size_q[0],round(time_exp_ms_query,3))
#print(result_query_mquery)
insert_query_mquery = f"INSERT INTO {m_query} (id_exp,db_exp,type_q,n_q,n_var,size_q,time_exp_ms) VALUES (%s, %s, %s, %s, %s, %s, %s)"
pg_cursor.execute(insert_query_mquery,result_query_mquery)
postgresdb_conn.commit()

#print("\n************************************************************************")
# Consulta rangos de tiempo, mediciones 1 dia n_q=10 ###########################################################################
n_q = 10

timestamp_tr_start = "'2019-01-22T00:00:00'"
timestamp_tr_end = "'2019-01-23T00:00:00'"

select_query_mquery = "SELECT device_id, timestamp, device_measurement FROM energy WHERE device_id IN ({}) AND timestamp >= {} AND timestamp < {} ORDER BY device_id, timestamp".format(records_template,timestamp_tr_start,timestamp_tr_end)

ts = time.time_ns()
ts_cursor.execute(select_query_mquery,devices_ids)
te = time.time_ns()
time_exp_ms_query = (te-ts)/1_000_000

result_query_select = ts_cursor.fetchall()
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
timestamp_tr_start = "'2019-04-16T00:00:00'"
timestamp_tr_end = "'2019-04-17T18:00:00'"

select_query_mquery = "SELECT device_id, timestamp, device_measurement FROM energy WHERE device_id IN ({}) AND timestamp >= {} AND timestamp < {} ORDER BY device_id, timestamp".format(records_template,timestamp_tr_start,timestamp_tr_end)

ts = time.time_ns()
ts_cursor.execute(select_query_mquery,devices_ids)
te = time.time_ns()
time_exp_ms_query = (te-ts)/1_000_000

result_query_select = ts_cursor.fetchall()
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
timestamp_tr_start = "'2019-01-01T00:00:00'"
timestamp_tr_end = "'2019-02-01T00:00:00'"

select_query_mquery = "SELECT device_id, timestamp, device_measurement FROM energy WHERE device_id IN ({}) AND timestamp >= {} AND timestamp < {} ORDER BY device_id, timestamp".format(records_template,timestamp_tr_start,timestamp_tr_end)

ts = time.time_ns()
ts_cursor.execute(select_query_mquery,devices_ids)
te = time.time_ns()
time_exp_ms_query = (te-ts)/1_000_000

result_query_select = ts_cursor.fetchall()
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
timestamp_tr_start = "'2019-01-01T00:00:00'"
timestamp_tr_end = "'2019-02-15T00:00:00'"

select_query_mquery = "SELECT device_id, timestamp, device_measurement FROM energy WHERE device_id IN ({}) AND timestamp >= {} AND timestamp < {} ORDER BY device_id, timestamp".format(records_template,timestamp_tr_start,timestamp_tr_end)

ts = time.time_ns()
ts_cursor.execute(select_query_mquery,devices_ids)
te = time.time_ns()
time_exp_ms_query = (te-ts)/1_000_000

result_query_select = ts_cursor.fetchall()
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
# Resultados

export_results_tojson(query_results,db_exp,str(size_exp),str(len(devices_ids)))

##########################################################################################################################################################
# Cerrar cursores y conexiones

ts_cursor.close()
pg_cursor.close()
timescaledb_conn.close()
postgresdb_conn.close()

print("\n#############\nFin de pruebas en TimescaleDB\n#############")
##########################################################################################################################################################
