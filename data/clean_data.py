import time, os, re
import numpy as np
import pandas as pd
from datetime import datetime

#################################################################################################################################

def clean_data(path_dir_data):
    data = pd.ExcelFile(path_dir_data)
    data = data.parse()
    c = 0
    c_i =0
    
    #Dataframe que almacenará el tipo de los valores de la columna timestamp y device_measurement
    type_values = pd.DataFrame(columns=['t_ts', 't_dev_m'])
    index_wrong = pd.DataFrame(columns=['i_wrong_ts', 'i_wrong_devm'])
    
    #Eliminar filas con valores NaN
    data = data.dropna()
    #Resetear indices de filas
    data = data.reset_index(drop=True)
    
    #Tipo de datos almacenados
    for i in range(len(data)):
        type_values.loc[c] = [type(data['timestamp'].iloc[i]),type(data['device_measurement'].iloc[i])]
        c+=1
    
    #print(type_values)
    
    #Conseguir indices de filas
    for index in range(len(type_values)):
        #print(type_values.loc[index,:])
        #Columna timestamp
        if(type_values.loc[index,'t_ts'] != datetime):
            index_wrong.loc[c_i,'i_wrong_ts'] = index
        
        if(type_values.loc[index,'t_dev_m'] == datetime or type_values.loc[index,'t_dev_m'] == str):
            index_wrong.loc[c_i,'i_wrong_devm'] = index
        
        c_i+=1
        #print(index_wrong)
        #print('\n***************************************************')
    #print('\n##################################')
    #print(index_wrong)
    
    #Eliminar filas que no tengan datetime en la columna timestamp
    data = data.drop(index_wrong.index)
    #Resetear indices de filas
    data = data.reset_index(drop=True)

    #Eliminar filas con un valor negativo en la medición del analizador de red
    data = data.drop(np.where(data['device_measurement']<0)[0])
    #Resetear indices de filas
    data = data.reset_index(drop=True)
    
    #Eliminamos filas con valores duplicados de la combinación identificador, fecha, medición y tipo de tag
    data = data.drop_duplicates(subset=['device_id','timestamp','device_measurement','type_of_tag'], keep='first')
    #Resetear indices de filas
    data = data.reset_index(drop=True)
    
    return data

#Funcion para separar el dataset pasado como parámetro,
#seleccionando el número de meses deseado, pasado como parámetro
def separate_dataset (dataset, n_months):
    data = dataset
    
    #Añadimos columna con la fecha de los datos
    data['date'] = data['timestamp'].dt.date
    #Cambiamos el tipo de datos para poder quedarnos solo con el mes y el año
    data = data.astype(dtype={'date': 'string'})
    
    #Eliminamos el dia de la fecha
    for i in range(len(data)):
        c_date = data['date'].iloc[i]
        data['date'].iloc[i] = c_date[:7]
    
    #Indexamos los datos por la columna date que contiene mes y año
    data.index = data['date']
    
    #Meses y años presentes en el dataset, ordenados
    range_dates = np.unique(data.index)
    
    data_s = data.loc[range_dates[:n_months]]
    data_s = data_s.drop(['date'], axis=1)

    return data_s

#################################################################################################################################


path_dir_data = "./data/silver_data.xlsx"
path_dir_golddata = "./data/gold_data"
path_dir_data_clean = "./data/gold_data/data_clean_all.xlsx"

print("\nComenzamos a limpiar los datos")
if not os.path.exists(path_dir_golddata):
    os.mkdir(path_dir_golddata)

if not os.path.exists(path_dir_data_clean):
    data_c = clean_data(path_dir_data)
    data_c.to_excel('./data/gold_data/data_clean_all.xlsx', index=False)

if (not os.path.exists('./data/gold_data/d_3.xlsx') or not os.path.exists('./data/gold_data/d_6.xlsx') or not os.path.exists('./data/gold_data/d_10.xlsx') or not os.path.exists('./data/gold_data/d_13.xlsx')):
    data_c = pd.ExcelFile(path_dir_data_clean)
    data_c = data_c.parse()

    d_3 = separate_dataset (data_c, 3)
    print("\n#######\nGeneramos archivo de 3 meses")
    d_3.to_excel('./data/gold_data/d_3.xlsx', index=False)

    d_6 = separate_dataset (data_c, 6)
    print("\n#######\nGeneramos archivo de 6 meses")
    d_6.to_excel('./data/gold_data/d_6.xlsx', index=False)

    d_10 = separate_dataset (data_c, 10)
    print("\n#######\nGeneramos archivo de 10 meses")
    d_10.to_excel('./data/gold_data/d_10.xlsx', index=False)

    d_13 = separate_dataset (data_c, 13)
    print("\n#######\nGeneramos archivo de 13 meses")
    d_13.to_excel('./data/gold_data/d_13.xlsx', index=False)

    print("\n#######\nFin de limpieza y separacion de datos en archivos de diferentes tamaños\n#######")

