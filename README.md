# TFG Grado Ingeniería Informática.

El objetivo de este proyecto es realizar un estudio sobre el rendimiento de diferentes bases de datos de series temporales.

¡Estos programas deben ser ejecutados en Linux!

Para ejecutar este proyecto se necesitan los archivos de este repositorio y el directorio con los datos originales que se tendrán que solicitar de manera privada si se dispone de los permisos necesarios.

### Pre-requisitos 📋

En el archivo 'requirements.txt' podemos encontrar las bibliotecas de Python que hemos utilizado durante el desarrollo del trabajo. Para instalarlas deberemos ejecutar el siguiente comando y si lo desea puede crear antes un entorno virtual para que no afecten estás versiones a otras que tenga instaladas en su máquina.

```
pip install -r requirements.txt
```

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

_Finaliza con un ejemplo de cómo obtener datos del sistema o como usarlos para una pequeña demo_

## Utilización de la aplicación ⚙️

_Explica como ejecutar las pruebas automatizadas para este sistema_

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
