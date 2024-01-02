import numpy as np
import pandas as pd
import os, re, time, json
from datetime import datetime
import plotly.express as px
import plotly.graph_objects as go

# Función que comprueba que existe la carpeta resultados
def path_res_exist(path_results):
    if os.path.exists(path_results):
        return True
    else:
        return False


# Función que, después de procesar la carpeta resultados, devuelve un dataframe que contiene un identificador, un tamaño en meses, un número de variables
# consultadas y una ruta
def existing_query_results(path):
    results_files = pd.DataFrame(columns=['Id','Size','Variables','Path'])
    c = 0
    l_paths = []
    
    for root, dirs, files in os.walk(path):
        for f in files:
            l_paths.append(os.path.join(root, f))
    
    for path in l_paths:
        r_id = re.search(r'/([^/]+)/[^/]+$', path)
        path_id = r_id.group(1)
        path_size = 0
        path_var = 0
        
        if (path_id != "results"):
            r_size = re.search(r'(\d+)data', path)
            path_size = r_size.group(1)
            
            r_var = re.search(r'(\d+)var', path)
            path_var = r_var.group(1)
            
        results_files.loc[c] = [path_id,int(path_size),int(path_var),path]
        c+= 1
    
    results_files = results_files.sort_values(by=['Id','Size','Variables'],ignore_index=True)

    return results_files

# Función que devuelve los resultados seleccionados
def select_results (dataframe_results, identifier, size, n_var):
    df_q = dataframe_results.loc[(dataframe_results['Id'] == identifier) & (dataframe_results['Size'] == size) & (dataframe_results['Variables'] == n_var)]
    
    if len(df_q) != 0:
        path_results_query = df_q.iat[0,3]
    else:
        path_results_query = None

    with open(path_results_query, 'r') as file:
        data= json.load(file)
        
    return data

# Función que devuelve un array de numpy con las consultas disponibles para el fichero de resultados seleccionado
def available_queries (dataframe_data):
    queries = np.array([(q['query'],q['info_query']) for q in dataframe_data])

    return queries

# Función que devuelve las opciones para los selects de base de datos, tamaño y variables
def options_template(dataframe_results):
    database_options = dataframe_results[dataframe_results['Id']!='results']['Id'].unique()
    size_options = dataframe_results[dataframe_results['Size']!=0]['Size'].unique()
    var_options = dataframe_results[dataframe_results['Variables']!=0]['Variables'].unique()

    return database_options, size_options, var_options

# Actualiza las opciones de los select para que muestren los últimos parámetros seleccionados
def update_selects_template(options_db, db, options_size, size, options_var, var, options_q, q):
    index_db = np.where(options_db == db)[0][0]
    options_db = np.concatenate([np.array([options_db[index_db]]), options_db[:index_db], options_db[index_db+1:]])

    index_size = np.where(options_size == int(size))[0][0]
    options_size = np.concatenate([np.array([options_size[index_size]]), options_size[:index_size], options_size[index_size+1:]])

    index_var = np.where(options_var == int(var))[0][0]
    options_var = np.concatenate([np.array([options_var[index_var]]), options_var[:index_var], options_var[index_var+1:]])

    index_q = np.where(options_q == q)[0][0]
    options_q = np.concatenate([np.array([options_q[index_q,:]]), options_q[:index_q,:], options_q[index_q+1:,:]])

    return options_db, options_size, options_var, options_q

# Función que devuelve los identificadores disponibles para los datos solicitados
def display_checkbox_ids(dataframe_data, query):
    device_ids = np.array([])

    for q in dataframe_data:
        if q['query'] == query:
            for r in q['results']:
                device_ids = np.append(device_ids, r['device_id'])
                
    device_ids_available = np.unique(device_ids)

    return device_ids_available

# Función que pasadas las opciones de identificadores y los identificadores que seleccionas
# te devuelve los identificadores que se pueden visualizar
# Por al pasar de más variables a menos en las opciones puede que se seleccionen variables
# que al actualizar la pagina reduciendo el numero de variables consultadas no estén disponibles
def dev_ids_tograph(devids_options, devids_checked):
    if(len(devids_checked) <= len(devids_options)):
        dev_ids_tg = devids_checked
    else:
        dev_ids_tg = []
        for d in devids_checked:
            if d in devids_options:
                dev_ids_tg.append(d)
        
    return dev_ids_tg

# Función que devuelve datos para graficar tras pasarle como parámetros un archivo con resultados de consultas
def plotting_data_queryresults(dataframe_data, value_query, dev_ids):
    data_plot = pd.DataFrame(columns=['device_id','value','timestamp'])
    c = 0
    for q in dataframe_data:
        # Recuperar datos de la consulta seleccionada
        if q['query'] == value_query:
            for r in q['results']:
                # Recuperar datos de los identificadores seleccionados
                if (r['device_id'] in dev_ids):
                    dev_id = r['device_id']
                    ts = r['timestamp']
                    if (ts == 'null'):
                        ts = c
                    # Porque hay alguna base de datos que almacena los resultados en una lista
                    if isinstance(r['value'],list):
                        for v in r['value']:
                            data_plot.loc[c] = [dev_id,v,ts]
                            c+=1
                    else:
                        data_plot.loc[c] = [dev_id,r['value'],ts]
                        c+=1
        
    
    return data_plot

# Función que genera un gráfico interactivo con los datos que se le pasan como parámetro
def generate_interactive_chart_qr(data, value_db, value_size, value_query, info):
    title = value_db + " -- " + str(value_size) + " meses <br>" + value_query + " -- " + info
    if not data.empty:
        fig = px.line(data, x=data['timestamp'], y=data['value'],
                        color=data['device_id'],
                        symbol=data['device_id'],
                        title=title,
                        markers=True)
        fig.update_layout(plot_bgcolor='white')
        fig.update_yaxes(title_text="Energía consumida",showline=True, linewidth=1, zerolinecolor='lightgrey', linecolor='black',gridcolor='lightgrey')
        fig.update_xaxes(title_text="Marca de tiempo",showline=True, linewidth=1, zerolinecolor='lightgrey', linecolor='black',gridcolor='lightgrey',zerolinewidth=1)

        # Gráfico a HTML
        chart_html = fig.to_html(full_html=False)

    else:
        fig = go.Figure()
        fig.add_annotation(
            text="No hay datos para mostrar",
            x=0.5,
            y=0.5,
            xref="paper",
            yref="paper",
            showarrow=False,
            font=dict(size=25))
        fig.update_xaxes(showticklabels=False)
        fig.update_yaxes(showticklabels=False)
        fig.update_layout(
            title=title,
            showlegend=False,
            xaxis=dict(showgrid=False, zeroline=False),
            yaxis=dict(showgrid=False, zeroline=False))

        # Gráfico a HTML
        chart_html = fig.to_html(full_html=False)

    return chart_html

###############################
###############################

# Para los resultados de rendimiento
# Función que devuelve las páginas del xlsx en diferentes variables
def select_performance_results (dataframe_results, identifier):
    df_p = dataframe_results[dataframe_results["Id"]==identifier]
    path_results_performance = df_p.iat[0,3]
    
    data_results = pd.ExcelFile(path_results_performance)
    m_data = data_results.parse("m_data")
    m_space = data_results.parse("m_space")
    m_query_3 = data_results.parse("m_query_3")
    m_query_6 = data_results.parse("m_query_6")
    m_query_10 = data_results.parse("m_query_10")
    m_query_13 = data_results.parse("m_query_13")
    
    return m_data, m_space, [m_query_3,m_query_6,m_query_10,m_query_13]

# Función que devuelve un array con las consultas disponibles
def query_options():

    query_options = [
    ["Consulta inserción", "Insertar datos en la base de datos"],
    ["Consulta almacenamiento", "Consultar el tamaño que ocupan los datos dentro de la base de datos"],
    ["q_1", "Obtener para un determinado sensor una medición de energía en un día y una hora determinados"],
    ["q_2", "Obtener para un determinado sensor la mínima medición de energía"],
    ["q_3", "Obtener para un determinado sensor la máxima medición de energía"],
    ["q_4", "Obtener para un determinado sensor la media de sus mediciones de energía"],
    ["q_5", "Obtener para un determinado sensor la desviación estándar de sus mediciones de energía"],
    ["q_6", "Obtener para un determinado sensor el conteo de sus mediciones de energía"],
    ["q_7", "Obtener para un determinado sensor los valores sin duplicados que toman sus mediciones de energía"],
    ["q_8_1", "Obtener para un determinado sensor la media de sus mediciones de energía por horas"],
    ["q_8_2", "Obtener para un determinado sensor la media de sus mediciones de energía por semanas"],
    ["q_8_3", "Obtener para un determinado sensor la media de sus mediciones de energía por meses"],
    ["q_9", "Obtener para un determinado sensor sus outliers o el tercer cuartil de sus mediciones"],
    ["q_10_1", "Obtener para un sensor determinado las mediciones de energía en un rango de tiempo determinado, un día"],
    ["q_10_2", "Obtener para un sensor determinado las mediciones de energía en un rango de tiempo determinado, un día y unas horas"],
    ["q_10_3", "Obtener para un sensor determinado las mediciones de energía en un rango de tiempo determinado, un mes"],
    ["q_10_4", "Obtener para un sensor determinado las mediciones de energía en un rango de tiempo determinado, un mes y unas semanas"]]

    return query_options

# Función que devuelve un dataframe para los resultados de carga de datos de "m_data"
def data_loading(dataframe):
    table_data_loading = pd.DataFrame(columns=['Base de datos','3 meses','6 meses','10 meses','13 meses'])
    c = 0
    for db in dataframe["db_exp"].unique():
        time_avg_per_size = []
        for size in dataframe["size_exp"].unique():
            df = dataframe[(dataframe["db_exp"]==db) & (dataframe["size_exp"]==size)]
            # En segundos
            time_avg_per_size.append(round(df["time_exp_ms"].mean()/1000,3))
        table_data_loading.loc[c] = [db] + time_avg_per_size
        c+=1
        
    return table_data_loading

# Función que devuelve un dataframe para los resultados de carga de datos de "m_space"
def storage(dataframe):
    table_storage = pd.DataFrame(columns=['Base de datos','3 meses','6 meses','10 meses','13 meses'])
    c = 0
    for db in dataframe["db_exp"].unique():
        mb_per_size = []
        for size in dataframe["size_exp"].unique():
            df = dataframe[(dataframe["db_exp"]==db) & (dataframe["size_exp"]==size)]
            # A veces hay una pequeña variación por lo que hacemos la media
            mb_per_size.append(round(df["space_exp_mb"].mean(),3))
        table_storage.loc[c] = [db] + mb_per_size
        c+=1
        
    return table_storage

# Devuelve un array con un dataframe para cada tamaño de datos
# Por cada base de datos tendremos la medición de la consulta en función de el rango que abarque para cada número de variables
def simple_query(m_query_per_size,n_q):
    size = [3,6,10,13]
    table_queries = pd.DataFrame(columns=['Base de datos','1 variable','5 variables','20 variables','Meses'])
    c = 0
    c_size = 0
    # Para cada tamaño de datos hay una hoja en el archivo xlsx
    for m_query in m_query_per_size:
        df = m_query[m_query["n_q"]==n_q]
        # Para cada base de datos cogemos los tiempos para cada número de variables
        for db in df["db_exp"].unique():
            df_db = df[df["db_exp"]==db]
            table_queries.loc[c] = [db] + df_db["time_exp_ms"].tolist() + [size[c_size]]
            c+=1
            
        c_size+=1
    
    return table_queries

# Devuelve un array con un dataframe para cada tamaño de datos
# Por cada base de datos tendremos la medición de la consulta en función de el rango que abarque para cada número de variables 
def complex_query(m_query_per_size,n_q):
    size = [3,6,10,13]
    table_queries_per_size = []
    c_size = 0
    # Para cada tamaño de datos hay una hoja en el archivo xlsx
    for m_query in m_query_per_size:
        c = 0
        table_queries = pd.DataFrame(columns=['Base de datos','1 variable','5 variables','20 variables','Rango','Meses'])
        df = m_query[m_query["n_q"]==n_q]
        # Para cada base de datos cogemos los tiempos para cada número de variables
        for s in df["size_q"].unique():
            df_size = df[df["size_q"]==s]
            
            for db in df_size["db_exp"].unique():
                df_size_db = df_size[df_size["db_exp"]==db]
                table_queries.loc[c] = [db] + df_size_db["time_exp_ms"].tolist() + [s] + [size[c_size]]
                c+=1
                
        c_size+=1
        table_queries_per_size.append(table_queries)

    return table_queries_per_size

# Funcion que dado un array con tamaños nos devuelve un array con los tamaños en el formato de las columnas
# de los dataframes de datos es decir si le pasamos [3] nos devuelve ["3 meses"]
def transform_size_value(sizes):
    new_sizes = []
    for s in sizes:
        ns = str(s) + " meses"
        new_sizes.append(ns)
    
    return new_sizes

# Función que devuelve la lista de consultas pero con la seleccionada en primer lugar
def update_query_select_pr(options_q, q):
    options_q = np.array(options_q)
    index_q = np.where(options_q == q)[0][0]
    options_q = np.concatenate([np.array([options_q[index_q,:]]), options_q[:index_q,:], options_q[index_q+1:,:]])
    
    return options_q

# Devuelve una lista con las opciones del número de variables consultadas de las que hay datos
def display_checkbox_vars(dataframes_query, value_query):
    if (value_query == "Consulta inserción" or value_query == "Consulta almacenamiento"):
        vars_options = []
    else:
        vars_options = dataframes_query[0]["n_var"].unique()

    return vars_options

# Función que transforma el identificador de la consulta seleccionada pasado como string
# en entero y la variable rango que nos permitirán obtener los datos de la tabla
def update_qid_to_complexquery(query_id):
    if (query_id[2]=="8"):
        q = 8
        if (query_id[-1] == "1"):
            range_q = "hour"
        elif(query_id[-1] == "2"):
            range_q = "week"
        elif(query_id[-1] == "3"):
            range_q = "month"
    elif((query_id[2:4]=="10")):
        q = 10
        if (query_id[-1] == "1"):
            range_q = "day"
        elif(query_id[-1] == "2"):
            range_q = "dayhour"
        elif(query_id[-1] == "3"):
            range_q = "month"
        elif(query_id[-1] == "4"):
            range_q = "monthweek"
    
    return q, range_q

# Funcion que pasados unos tamaños me actualiza los valores para localizar los datos
# en la lista de dataframes
def update_size_to_complexquery(size_list):
    new_sizes = []
    for size in size_list:
        if (size == 3):
            new_sizes.append(0)
        elif(size == 6):
            new_sizes.append(1)
        elif(size == 10):
            new_sizes.append(2)
        elif(size==13):
            new_sizes.append(3)

    return new_sizes

# Función que devuelve datos para graficar tras pasarle como parámetros unos dataframes con datos, la consulta, las bases de datos,
# el tamaño y las variables seleccionadas
def plotting_data_performanceresults(dataframe_data, dataframe_space, dataframes_queries, databases, sizes, value_query, variables):
    n_variables = []
    for i in range(len(variables)):
        if variables[i]==1:
            n_variables.append(str(variables[i])+" variable")
        else:
            n_variables.append(str(variables[i])+" variables")
            
    if (value_query == "Consulta inserción"):
        sizes = transform_size_value(sizes)
        if (len(sizes) == 0):
            df = 0
        else:
            df = data_loading(dataframe_data)
            df = df[df["Base de datos"].isin(databases)]
            columns_selected = ["Base de datos"] + sizes
            df = df[columns_selected]
    
    elif (value_query == "Consulta almacenamiento"):
        sizes = transform_size_value(sizes)
        if (len(sizes) == 0):
            df = 0
        else:
            rows = []
            for db in databases:
                js = "json"+db
                rows.append(db)
                if(db == "opentsdb"):
                    rows.append(js[:-4])
                else:
                    rows.append(js[:-2])
            df = storage(dataframe_space)
            df = df[df["Base de datos"].isin(rows)]
            columns_selected = ["Base de datos"] + sizes
            df = df[columns_selected]
    
    elif (value_query in ["q_1", "q_2", "q_3", "q_4", "q_5", "q_6", "q_7", "q_9"]):
        if (len(n_variables) == 0 or len(sizes) == 0):
            df = 0
        else:
            q = int(value_query[-1])
            df = simple_query(dataframes_queries,q)
            df = df[df["Base de datos"].isin(databases)]
            df = df[df["Meses"].isin(sizes)]
            df = df[["Base de datos"]+n_variables+["Meses"]]
    else:
        if (len(n_variables) == 0 or len(sizes) == 0):
            df = 0
        else:
            q, range_q = update_qid_to_complexquery(value_query)
            df1 = complex_query(dataframes_queries,q)
            index_sizes = update_size_to_complexquery(sizes)
            df = pd.concat([df1[i] for i in index_sizes])
            df = df[df["Base de datos"].isin(databases)]
            df = df[df["Rango"]==range_q]
            df = df[["Base de datos"]+n_variables+["Rango","Meses"]]
            
    return df

# Función que genera el gráfico interactivo para los resultados de las pruebas de rendimiento
def generate_interactive_chart_pr(data, value_query, query_options):
    for q in query_options:
        if (value_query == q[0]):
            info_q = q[1]

    title = value_query + " -- " + info_q

    if (isinstance(data,int)):
        fig = go.Figure()
        fig.add_annotation(
            text="Seleccione variables para mostrar datos",
            x=0.5,
            y=0.5,
            xref="paper",
            yref="paper",
            showarrow=False,
            font=dict(size=25))
        fig.update_xaxes(showticklabels=False)
        fig.update_yaxes(showticklabels=False)
        fig.update_layout(
            title="",
            showlegend=False,
            xaxis=dict(showgrid=False, zeroline=False),
            yaxis=dict(showgrid=False, zeroline=False))

        # Gráfico a HTML
        chart_html = fig.to_html(full_html=False)

    else:
        if(value_query in ["Consulta inserción","Consulta almacenamiento"]):
            y_title = "Latencia consulta (ms)"
            if(value_query=="Consulta almacenamiento"):
                y_title = "Espacio consumido (MB)"

            df = pd.melt(data, id_vars=["Base de datos"], var_name=["Meses"], value_name="Tiempo")

            fig = px.histogram(df, y="Tiempo", x="Meses", color="Base de datos", barmode='group', text_auto=True)
            fig.update_layout(
                title_text= title,
                height=500,
                yaxis_title_text=y_title,
                xaxis_title_text='',
                xaxis=dict(tickfont=dict(size=14,color='black')),
                bargap=0.1,
                bargroupgap=0.1,
                plot_bgcolor='white',
                legend=dict(y=-0.08,x=-0.05, orientation='h', font=dict(size=15,color='black')),
                font=dict(
                    size=15
                ))
            fig.update_yaxes(showline=True, linewidth=1, linecolor='black',gridcolor='lightgrey')
            fig.update_xaxes(showline=True, linewidth=0.5, linecolor='black')
            fig.update_traces(textfont_size=15, textangle=0, textposition="outside", cliponaxis=False)

            # Gráfico a HTML
            chart_html = fig.to_html(full_html=False)

        elif(value_query in ["q_1", "q_2", "q_3", "q_4", "q_5", "q_6", "q_7", "q_9"]):
            df = pd.melt(data, id_vars=["Base de datos", "Meses"], var_name=["Variables"], value_name="Tiempo")
            df["Meses"] = df["Meses"].astype(str) + " meses"

            fig = px.histogram(df, y="Tiempo", x="Meses", color="Base de datos", barmode='group', facet_row="Variables", text_auto=True)
            fig.update_layout(
                title_text= title,
                height=800,
                xaxis=dict(tickfont=dict(size=14,color='black')),
                bargap=0.1,
                bargroupgap=0.1,
                plot_bgcolor='white',
                legend=dict(y=-0.08,x=-0.05, orientation='h', font=dict(size=15,color='black')),
                font=dict(
                    size=15
                ))
            fig.update_xaxes(title_text="",showline=True, linewidth=0.5, linecolor='black')
            fig.update_yaxes(title_text="Latencia consulta (ms)",showline=True, linewidth=1, linecolor='black',gridcolor='lightgrey')
            fig.update_traces(textfont_size=15, textangle=0, textposition="outside", cliponaxis=False)
            fig.for_each_annotation(lambda a: a.update(text=a.text.replace("Variables=", "")))

            # Gráfico a HTML
            chart_html = fig.to_html(full_html=False)

        else:
            data = data.drop("Rango", axis=1)
            df = pd.melt(data, id_vars=["Base de datos", "Meses"], var_name=["Variables"], value_name="Tiempo")
            df["Meses"] = df["Meses"].astype(str) + " meses"

            fig = px.histogram(df, y="Tiempo", x="Meses", color="Base de datos", barmode='group', facet_row="Variables", text_auto=True)
            fig.update_layout(
                height=800,
                title_text= title,
                xaxis=dict(tickfont=dict(size=14,color='black')),
                bargap=0.1,
                bargroupgap=0.1,
                plot_bgcolor='white',
                legend=dict(y=-0.08,x=-0.05, orientation='h', font=dict(size=15,color='black')),
                font=dict(
                    size=15
                ))
            fig.update_xaxes(title_text="",showline=True, linewidth=0.5, linecolor='black')
            fig.update_yaxes(title_text="Latencia consulta (ms)",showline=True, linewidth=1, linecolor='black',gridcolor='lightgrey')
            fig.update_traces(textfont_size=15, textangle=0, textposition="outside", cliponaxis=False,)
            fig.for_each_annotation(lambda a: a.update(text=a.text.replace("Variables=", "")))

            # Gráfico a HTML
            chart_html = fig.to_html(full_html=False)

    return chart_html