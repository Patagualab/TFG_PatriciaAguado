#!/bin/bash

###############################################################################################
###############################################################################################

# Obtiene la ubicación del script actual
SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"

# Ruta al archivo docker-compose
DOCKER_COMPOSE_FILE="$SCRIPT_DIR/docker/docker-compose.yml"

#####################################################

# Contraseña para PostgreSQL y TimescaleDB
PGPASSWORD="admin"

#####################################################

# Contenedor PostgreSQL
DB_CONTAINER_PSQL="postgres_db_container"
DB_SERVICE_PSQL="postgresdb"

# PostgreSQL
DB_HOST_PSQL="localhost"
DB_PORT_PSQL="5432"
DB_NAME_PSQL="postgres_db"
DB_USER_PSQL="admin"
SQL_FILE_PSQL="$SCRIPT_DIR/docker/scripts_sql/postgresdb.sql"

#####################################################

# Contenedor TimescaleDB
DB_CONTAINER_TSQL="timescale_db_container"
DB_SERVICE_TSQL="timescaledb"

# TimescaleDB
DB_HOST_TSQL="localhost"
DB_PORT_TSQL="5434"
DB_NAME_TSQL="timescale_db"
DB_USER_TSQL="admin"
SQL_FILE_TSQL="$SCRIPT_DIR/docker/scripts_sql/timescaledb.sql"

#####################################################

# Contenedor InfluxDB
DB_CONTAINER_INFLUX="influx_db_container"
DB_SERVICE_INFLUX="influxdb"

#####################################################

# Contenedor OpentsDB
DB_CONTAINER_OPEN="opents_db_container"
DB_SERVICE_OPEN="opentsdb"

#####################################################

# Contenedor KairosDB
DB_CONTAINER_KAIROS="kairos_db_container"
DB_SERVICE_KAIROS="kairosdb"

DB_CONTAINER_CASSANDRA="cassandra_db_container"
DB_SERVICE_CASSANDRA="cassandra"

###############################################################################################
###############################################################################################
#Funciones

reboot_enviroment_timescaledb(){
    docker stop $DB_CONTAINER_TSQL
    docker rm $DB_CONTAINER_TSQL
    docker volume prune -f
    docker-compose -f $DOCKER_COMPOSE_FILE up -d $DB_SERVICE_TSQL
    docker cp $SQL_FILE_TSQL $DB_CONTAINER_TSQL:/timescaledb.sql
    docker exec -it $DB_CONTAINER_TSQL bash -c "echo \"shared_preload_libraries = 'pg_stat_statements,timescaledb'\" >> /var/lib/postgresql/data/postgresql.conf"
    docker restart $DB_CONTAINER_TSQL
    sleep 10
    docker exec -it $DB_CONTAINER_TSQL psql -h $DB_HOST_TSQL -U $DB_USER_TSQL -d $DB_NAME_TSQL -f "/timescaledb.sql"
}

reboot_enviroment_influxdb(){
    docker stop $DB_CONTAINER_INFLUX
    docker rm $DB_CONTAINER_INFLUX
    docker volume prune -f
    docker-compose -f $DOCKER_COMPOSE_FILE up -d $DB_SERVICE_INFLUX
}

reboot_enviroment_opentsdb(){
    docker stop $DB_CONTAINER_OPEN
    sleep 10
    docker rm $DB_CONTAINER_OPEN
    docker volume prune -f
    docker-compose -f $DOCKER_COMPOSE_FILE up -d $DB_SERVICE_OPEN
    docker exec -it $DB_CONTAINER_OPEN bash -c "echo \"tsd.query.skip_unresolved_tagvs = true\" >> /usr/local/share/opentsdb/etc/opentsdb/opentsdb.conf"
    sleep 5
    docker restart $DB_CONTAINER_OPEN
    sleep 10
}

reboot_enviroment_kairoscassandra(){
    docker stop $DB_CONTAINER_KAIROS
    docker stop $DB_CONTAINER_CASSANDRA
    docker rm $DB_CONTAINER_KAIROS
    docker rm $DB_CONTAINER_CASSANDRA
    docker volume prune -f
    sleep 5
    docker-compose -f $DOCKER_COMPOSE_FILE up -d $DB_SERVICE_CASSANDRA
    docker-compose -f $DOCKER_COMPOSE_FILE up -d $DB_SERVICE_KAIROS
    sleep 10
}

###############################################################################################
###############################################################################################

# Variables para pruebas
DATA_SIZE=(3 6 10 13)
N_VAR=(1 5 20)

# Script para resetar la base de datos con los resultados
SCRIPT_PY_RESETDB="$SCRIPT_DIR/docker/scripts_python/reset_database_psql.py"

# Scripts de pruebas
SCRIPT_PY_INFLUXDB="$SCRIPT_DIR/docker/scripts_python/influxdb.py"
SCRIPT_PY_TIMESCALEDB="$SCRIPT_DIR/docker/scripts_python/timescaledb.py"
SCRIPT_PY_OPENTSDB="$SCRIPT_DIR/docker/scripts_python/opentsdb.py"
SCRIPT_PY_KAIROSDB="$SCRIPT_DIR/docker/scripts_python/kairosdb.py"

# Script de resultados
SCRIPT_RESULTS_PERFORMANCE="$SCRIPT_DIR/docker/scripts_python/results_performance.py"

###############################################################################################
###############################################################################################

# Docker
sudo systemctl start docker
sleep 5

# Iniciar base de datos que almacenará resultados
docker-compose -f $DOCKER_COMPOSE_FILE up -d $DB_SERVICE_PSQL
docker cp $SQL_FILE_PSQL $DB_CONTAINER_PSQL:/postgresdb.sql
sleep 10
docker exec -it $DB_CONTAINER_PSQL psql -h $DB_HOST_PSQL -U $DB_USER_PSQL -d $DB_NAME_PSQL -f "/postgresdb.sql"
sleep 5

python3 "$SCRIPT_PY_RESETDB"

###############################################################################################
###############################################################################################

# TimescaleDB
docker-compose -f $DOCKER_COMPOSE_FILE up -d $DB_SERVICE_TSQL
docker cp $SQL_FILE_TSQL $DB_CONTAINER_TSQL:/timescaledb.sql
sleep 5
docker exec -it $DB_CONTAINER_TSQL bash -c "echo \"shared_preload_libraries = 'pg_stat_statements,timescaledb'\" >> /var/lib/postgresql/data/postgresql.conf"
sleep 5
docker restart $DB_CONTAINER_TSQL
sleep 10
docker exec -it $DB_CONTAINER_TSQL psql -h $DB_HOST_TSQL -U $DB_USER_TSQL -d $DB_NAME_TSQL -f "/timescaledb.sql"

# TimescaleDB performance

for SIZE in ${DATA_SIZE[@]}; do
    for VAR in ${N_VAR[@]}; do
        python3 "$SCRIPT_PY_TIMESCALEDB" $VAR $SIZE
	echo -e "\n"
        echo "Pruebas para $VAR identificador/es y $SIZE meses, realizadas"
        reboot_enviroment_timescaledb
    done
done

docker stop $DB_CONTAINER_TSQL
docker rm $DB_CONTAINER_TSQL
docker volume prune -f

###############################################################################################
###############################################################################################

# InfluxDB
docker-compose -f $DOCKER_COMPOSE_FILE up -d $DB_SERVICE_INFLUX

# InfluxDB performance

for SIZE in ${DATA_SIZE[@]}; do
    for VAR in ${N_VAR[@]}; do
        python3 "$SCRIPT_PY_INFLUXDB" $VAR $SIZE
	echo -e "\n"
        echo "Pruebas para $VAR identificador/es y $SIZE meses, realizadas"
        reboot_enviroment_influxdb
    done
done

docker stop $DB_CONTAINER_INFLUX
docker rm $DB_CONTAINER_INFLUX
docker volume prune -f

###############################################################################################
###############################################################################################

# OpentsDB
docker-compose -f $DOCKER_COMPOSE_FILE up -d $DB_SERVICE_OPEN
docker exec -it $DB_CONTAINER_OPEN bash -c "echo \"tsd.query.skip_unresolved_tagvs = true\" >> /usr/local/share/opentsdb/etc/opentsdb/opentsdb.conf"
sleep 5
docker restart $DB_CONTAINER_OPEN
sleep 10

# OpentsDB performance
for SIZE in ${DATA_SIZE[@]}; do
    for VAR in ${N_VAR[@]}; do
        python3 "$SCRIPT_PY_OPENTSDB" $VAR $SIZE
        echo -e "\n"
	echo "Pruebas para $VAR identificador/es y $SIZE meses, realizadas"
        reboot_enviroment_opentsdb
    done
done

docker stop $DB_CONTAINER_OPEN
sleep 10
docker rm $DB_CONTAINER_OPEN
docker volume prune -f

###############################################################################################
###############################################################################################

# KairosDB
docker-compose -f $DOCKER_COMPOSE_FILE up -d $DB_SERVICE_CASSANDRA
docker-compose -f $DOCKER_COMPOSE_FILE up -d $DB_SERVICE_KAIROS

# KairosDB performance
for SIZE in ${DATA_SIZE[@]}; do
    for VAR in ${N_VAR[@]}; do
        python3 "$SCRIPT_PY_KAIROSDB" $VAR $SIZE
        echo -e "\n"
	echo "Pruebas para $VAR identificador/es y $SIZE meses, realizadas"
        reboot_enviroment_kairoscassandra
    done
done

docker stop $DB_CONTAINER_KAIROS
docker stop $DB_CONTAINER_CASSANDRA
docker rm $DB_CONTAINER_KAIROS
docker rm $DB_CONTAINER_CASSANDRA
docker volume prune -f

###############################################################################################
###############################################################################################

# Sacar los resultados de la base de datos PostgresDB
python3 "$SCRIPT_RESULTS_PERFORMANCE" "$SCRIPT_DIR/docker/results/results_performance.xlsx"
