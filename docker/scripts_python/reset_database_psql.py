import psycopg2

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

#############################################################################

# Borrar contenido de tablas

pg_cursor.execute("DELETE from m_data")
pg_cursor.execute("DELETE from m_space")
pg_cursor.execute("DELETE from m_query_3")
pg_cursor.execute("DELETE from m_query_6")
pg_cursor.execute("DELETE from m_query_10")
pg_cursor.execute("DELETE from m_query_13")

postgresdb_conn.commit()
pg_cursor.close()
postgresdb_conn.close()

print("\n########\nLimpieza base de datos PostgreSQL completada\n#######")
