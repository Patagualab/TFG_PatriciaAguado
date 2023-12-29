import time, os ,re
import numpy as np
import pandas as pd
from datetime import datetime

#################################################################################################################################

# Devuelve un array con los ficheros que hay en el directorio pasado como parámetro
def list_dir (path_directory):
    l_datafiles = []

    try:
        l_dir = os.walk(path_directory)
        for i in l_dir:
            with os.scandir(i[0]) as df:
                for j in df:
                    if j.is_file():
                        l_datafiles.append(i[0]+'/'+j.name)

    except OSError as e:
        print(f"\nError al buscar ficheros de datos: {e}")

    finally:
        return l_datafiles

#Dado un string del tipo ditech://LUCIA:device_id (, o .) seguido de mas texto, obtenemos el identificador del sensor
def process_device_id (analyser_name):
    #Expresion regular para el tag
    r_device_id = re.compile('LUCIA:.*PrVal')
    device_id = r_device_id.search(analyser_name).group(0)
    #Eliminamos de la cadena LUCIA: y el final que es un signo de puntuación (. o ,) y PrVal
    device_id = device_id[6:len(device_id)-6]
    
    return device_id

#Tenemos la información de un analizador de red duplicada con la diferencia de un signo de puntuación
#Buscamos en la cadena de información una coma ',' y la cambiamos por un punto '.''
def process_information (information_str):
    a,b = ',','.'
    trans = str.maketrans(a,b)
    new_info = information_str.translate(trans)
    
    return new_info

#Dada una fecha de tipo datetime la convierte a formato UNIX TimeStamp
def process_datetime_measurement (date):
    date_unix = time.mktime(date.timetuple())
    
    return date_unix

def read_sheets (array_files):
    devices_ids = pd.DataFrame(columns=['device_id','unit_measurement','information'])
    devices_data = pd.DataFrame(columns=['device_id','timestamp','device_measurement','unit_measurement','type_of_tag','information'])
    
    #Contador de identificador de sensores
    c = 0
    #Contador de lectura de valor
    c_dm = 0
    
    for file_name in array_files:
        print(f"\nSe va a procesar el fichero de datos: {file_name}\n")
        f = pd.ExcelFile(file_name)
        sheets = f.sheet_names
        
        #No leemos la hoja del resumen
        for s in sheets[1:]:
            #print(s)
            #Dataframe de la hoja del excel
            sheet_data = f.parse(s)

            #Cada 3 columnas encontramos el tag del analizador de red
            for j in range(0,sheet_data.shape[1],3):
                #print(sheet_data.columns[j])
                #Para no meter la celda vacia despues del ultimo tag de la hoja
                if (len(sheet_data.columns[j]) > 11):
                    #tag de identificador de sensor y localización
                    device_id = process_device_id(sheet_data.columns[j])
                    information = sheet_data.iloc[0,j]
                    if (',' in information):
                        information = process_information(information)
                        
                    unit_measurement = sheet_data.iloc[1,j+1]
                    
                    #Si no habiamos guardado el identificador del sensor lo añadimos
                    if(device_id not in devices_ids.values):
                        devices_ids.loc[c] = [device_id,unit_measurement,information]
                        c += 1
                    
                    measurements_device = sheet_data.iloc[2:,j:j+3]
                    #print(len(measurements_device.columns))
                    
                    if (len(measurements_device.columns)<3):
                        measurements_device['type_t'] = np.full((measurements_device.shape[0], 1), np.nan)
                        
                    #Añadimos nombres de columnas para borrar valores Nan luego filtrando por fecha
                    measurements_device.columns = ['ts','m','type_t']

                    measurements_device = measurements_device.dropna(subset='ts')

                    measurements_device['type_t'] = measurements_device['type_t'].fillna("Desconocido")

                    
                    #Leer filas de toma de valores
                    for i in range(measurements_device.shape[0]):
                        timestamp = measurements_device.iloc[i,0]
                        device_m = measurements_device.iloc[i,1]
                        t_tag = measurements_device.iloc[i,2]
                        
                        devices_data.loc[c_dm] = [device_id,timestamp,device_m,unit_measurement,t_tag,information]
                        c_dm += 1
                    
                    print(f"\nSe han procesado {c_dm} registros")
                        
        f.close()
    
    return devices_ids, devices_data

#################################################################################################################################

path_dir_data = "./data/raw_data"
datafiles = list_dir(path_dir_data)

if datafiles and not os.path.exists('./data/silver_data.xlsx'):
    devices_ids, devices_data = read_sheets(datafiles)
    devices_data.to_excel('./data/silver_data.xlsx', index=False)
    print("\n#############\nLectura y fusion de datos finalizada\n#############")

elif os.path.exists('./data/silver_data.xlsx'):
    print("\n#############\nYa existe un fichero con datos procesados\n#############")

else:
    print("\n#############\nNo se pudieron procesar los ficheros de raw_data\n#############")
