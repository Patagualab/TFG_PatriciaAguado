# TFG Grado Ingeniería Informática.

El objetivo de este proyecto es realizar un estudio sobre el rendimiento de diferentes bases de datos de series temporales.

¡Estos programas deben ser ejecutados en Linux!

Para ejecutar este proyecto se necesitan los archivos de este repositorio y el directorio con los datos originales que se tendrán que solicitar de manera privada si se dispone de los permisos necesarios.

### Instalación 🔧

Para ejecutar este proyecto debemos instalar las siguientes herramientas de software:

Deberemos comprobar que la versión que tenemos de Python es la 3 y tener el instalador de paquetes pip. Para ello podrá ejecutar las siguientes líneas de código en su terminal de Linux.
```
sudo apt install python3
sudo apt install pip
```
También tendremos que instalar [Docker](https://docs.docker.com/engine/install/ubuntu/). Para utilizar esta herramienta sin privilegios de superusuario podemos ejecutar lo siguiente:

```
sudo groupadd docker
sudo usermod -aG docker $USER
sudo systemctl restart docker
sudo chmod 666 /var/run/docker.sock
```

### Pre-requisitos 📋

En el archivo 'requirements.txt' podemos encontrar las bibliotecas de Python que hemos utilizado durante el desarrollo del trabajo. Para instalarlas deberemos ejecutar el siguiente comando y si lo desea puede crear antes un entorno virtual para que no afecten estás versiones a otras que tenga instaladas en su máquina.

```
pip install -r requirements.txt
```
## Ejecución 💻

Teniendo en cuenta que hemos seguido los pasos anteriores y tenemos todo el software necesario, el primer paso será descargar este repositorio y obtener la carpeta 'raw_data' con los datos originales que ubicaremos en 'TFG_PatriciaAguado/data/'.

A continuación, tendremos que dar permisos de ejecución al usuario para los scripts de bash:

```
sudo chmod u+x docker_db.sh
sudo chmod u+x process_data.sh
sudo chmod u+x visualization.sh
```
En primer lugar, abrimos una terminal en 'TFG_PatriciaAguado' y ejecutamos el script 'process_data.sh':
```
./process_data.sh
```
Este se encarga de ejecutar en orden los programas de Python que se encuentran en el directorio 'data'. Estos consisten en:
* 'read_data.py': leer todos los archivos que se encuentran en el directorio 'data/raw_data' y juntarlos para generar el archivo 'data/silver_data.xlsx'.
* 'clean_data.py': realizar sobre 'silver_data.xlsx' un proceso de limpieza de datos y generar en el directorio 'data/gold_data'} un archivo con todos los datos de calidad y uno por cada tamaño especificado (3, 6, 10 y 13 meses).
* 'json_schemas.py': crear un directorio 'data/json_schemas', en el que se crea un directorio por cada base de datos y en cada uno de ellos un archivo de formato JSON para cada tamaño.
  
En segundo lugar, una vez que ya tenemos todos los archivos preparados con los datos que queremos insertar para las pruebas de rendimiento en el directorio 'data/json_schemas' ejecutamos el script 'docker_db.sh':
```
./docker_db.sh
```
Este primero monta un contenedor de Docker para nuestra base de datos personal, a partir del script SQL 'docker/scripts_sql/postgresdb.sql'. Como esta base de datos almacenará los resultados, se genera un directorio de persistencia de datos denominado 'docker/postgresdb'.

Después, el script continúa montando un contenedor de Docker, realizando pruebas de rendimiento (es decir, ejecutando los programas python que se encuentran en 'docker/scripts_python), guardando los datos recuperados en cada consulta, insertando los resultados de rendimiento en nuestra base de datos y desmontando el contenedor para cada servicio definido en el archivo 'docker/docker-compose.yml' de forma recursiva.

Al finalizar, se ejecuta el programa 'results_performance.py' que recupera los resultados de PostgreSQL en un documento XLSX que contiene las tablas con los resultados de las pruebas de rendimiento ('results_performance.xlsx') y que almacena en el directorio 'results' junto con los resultados de cada consulta en cada experimento.

En último lugar, ejecutamos el script 'visualization.sh' que nos abrirá una página en nuestro navegador por defecto con el puerto 5000, que es en el que se encuentra el servidor de nuestra aplicación web de visualización de datos:
```
./visualization.sh
```


## Utilización de la aplicación ⚙️



## Construido con 🛠️

Las herramientas principales que hemos utilizado para llevar acabo este proyecto son las siguientes:

* [Python](https://www.python.org/) - Lenguaje de programación
* [Docker](https://www.docker.com/) - Plataforma basada en la virtualización de entornos aislados para facilitar el despliegue de aplicaciones
* [Sublime Text](https://www.sublimetext.com/) - Usado para generar todos los scripts del trabajo
* [Flask](https://flask.palletsprojects.com/en/3.0.x/) - Usado para la aplicación web
* [Plotly](https://plotly.com/python/) - Usado para generar gráficos interactivos 

## Imagenes utilizadas en Docker 🖼️

Las imágenes de Docker utilizadas para definir los servicios de bases de datos en el archivo docker-compose.yml son las siguientes:

* [PostgreSQL](https://hub.docker.com/_/postgres) - Imagen oficial de Docker para PostgreSQL
* [InfluxDB](https://hub.docker.com/_/influxdb) - Imagen oficial de Docker para InfluxDB
* [TimescaleDB](https://hub.docker.com/r/timescale/timescaledb) - Imagen de Docker para TimescaleDB
* [OpenTSDB](https://hub.docker.com/r/petergrace/opentsdb-docker) - Imagen de Docker para OpenTSDB
* [KairosDB](https://hub.docker.com/r/elastisys/kairosdb) - Imagen de Docker para KairosDB
* [Cassandra](https://hub.docker.com/_/cassandra) - Imagen oficial de Docker para Cassandra

## Autores ✒️

* **Patricia Aguado Labrador** - [Patagualab](https://github.com/Patagualab)
