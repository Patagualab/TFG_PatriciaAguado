import psycopg2
import pandas as pd
import docker
import sys

#############################################################################

path_results_performance = str(sys.argv[1])

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

postgresdb_conn = psycopg2.connect(
    host=db_host_psql,
    port=db_port_psql,
    user=db_user_psql,
    password=db_pssw_psql,
    database=db_name_psql
)

pg_cursor = postgresdb_conn.cursor()

client_docker = docker.DockerClient(base_url='unix://var/run/docker.sock')
client_docker.containers.get(db_container_psql).start()

#########################################################################################
################################      TABLAS ENTERAS     ################################
#########################################################################################

#### m_data #############################################################################

df_m_data = pd.DataFrame(columns=["id_exp", "db_exp", "size_exp", "time_exp_ms"])

select_query = "SELECT * FROM m_data"
pg_cursor.execute(select_query)
result_query_select = pg_cursor.fetchall()

c = 0
for r in result_query_select:
    df_m_data.loc[c] = r
    c+=1
#print(df_m_data)

#### m_space ############################################################################

df_m_space = pd.DataFrame(columns=["id_exp", "db_exp", "size_exp", "space_exp_mb"])

select_query = "SELECT * FROM m_space"
pg_cursor.execute(select_query)
result_query_select = pg_cursor.fetchall()

c = 0
for r in result_query_select:
    df_m_space.loc[c] = r
    c+=1
#print(df_m_space)

#### m_query_3 ##########################################################################

df_m_query_3 = pd.DataFrame(columns=["id_exp", "db_exp", "type_q", "n_q", "n_var", "size_q", "time_exp_ms"])

select_query = "SELECT * FROM m_query_3"
pg_cursor.execute(select_query)
result_query_select = pg_cursor.fetchall()

c = 0
for r in result_query_select:
    df_m_query_3.loc[c] = r
    c+=1
#print(df_m_query_3)

#### m_query_6 ##########################################################################

df_m_query_6 = pd.DataFrame(columns=["id_exp", "db_exp", "type_q", "n_q", "n_var", "size_q", "time_exp_ms"])

select_query = "SELECT * FROM m_query_6"
pg_cursor.execute(select_query)
result_query_select = pg_cursor.fetchall()

c = 0
for r in result_query_select:
    df_m_query_6.loc[c] = r
    c+=1
#print(df_m_query_6)

#### m_query_10 #########################################################################

df_m_query_10 = pd.DataFrame(columns=["id_exp", "db_exp", "type_q", "n_q", "n_var", "size_q", "time_exp_ms"])

select_query = "SELECT * FROM m_query_10"
pg_cursor.execute(select_query)
result_query_select = pg_cursor.fetchall()

c = 0
for r in result_query_select:
    df_m_query_10.loc[c] = r
    c+=1
#print(df_m_query_10)

#### m_query_13 #########################################################################

df_m_query_13 = pd.DataFrame(columns=["id_exp", "db_exp", "type_q", "n_q", "n_var", "size_q", "time_exp_ms"])

select_query = "SELECT * FROM m_query_13"
pg_cursor.execute(select_query)
result_query_select = pg_cursor.fetchall()

c = 0
for r in result_query_select:
    df_m_query_13.loc[c] = r
    c+=1
#print(df_m_query_13)


with pd.ExcelWriter(path_results_performance) as writer:
    df_m_data.to_excel(writer, sheet_name='m_data')
    df_m_space.to_excel(writer, sheet_name='m_space')
    df_m_query_3.to_excel(writer, sheet_name='m_query_3')
    df_m_query_6.to_excel(writer, sheet_name='m_query_6')
    df_m_query_10.to_excel(writer, sheet_name='m_query_10')
    df_m_query_13.to_excel(writer, sheet_name='m_query_13')

print("\n#######\nResultados de pruebas exportados a Excel\n#######")
