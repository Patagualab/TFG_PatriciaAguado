from flask import render_template, request
from flask import Flask

app = Flask(__name__)

import controller, models

@app.route('/')
def index():
	return render_template('index.html')

@app.route('/queryresults', methods=['GET', 'POST'])
def queryresults():
    path_dir_results = "../docker/results"

    if not models.path_res_exist(path_dir_results):
        return render_template('error.html')

    results_df = models.existing_query_results(path_dir_results)
    # Porque results contiene los resultados de rendimiento
    result = results_df[results_df['Id']!='results'].iloc[0]
    value_db = result['Id']
    value_size = result['Size']
    value_var = result['Variables']
    
    data = models.select_results (results_df, value_db, value_size, value_var)
    query_options = models.available_queries(data)
    
    value_query = query_options[0][0]
    info_query = query_options[0][1]
    database_options, size_options, var_options = models.options_template(results_df)
    dev_ids_options = models.display_checkbox_ids(data, query_options[0][0])

    if request.method == 'POST':
        value_db = request.form.get("db")
        value_size = request.form.get("s")
        value_var = request.form.get("var")

        data = models.select_results (results_df, value_db, int(value_size), int(value_var))
        query_options = models.available_queries(data)
        value_query = request.form.get("q")

        # Opciones con los identificadores disponibles
        dev_ids_options = models.display_checkbox_ids(data, value_query)

        # Identificadores seleccionados para visualizar
        opt_devids_check = request.form.getlist('opt_devids_check')

        # Obtener identificadores disponibles para los parámetros relacionados
        dev_ids = models.dev_ids_tograph(dev_ids_options, opt_devids_check)

        # Actualización de parámetros para que salgan los últimos seleccionados
        database_options, size_options, var_options, query_options = models.update_selects_template(database_options, value_db, size_options, value_size, var_options, value_var, query_options, value_query)
        info_query = query_options[0][1]
        
        # Datos para graficar
        data_plot = models.plotting_data_queryresults(data, value_query, dev_ids)

        # Generar gráfico
        chart = models.generate_interactive_chart_qr(data_plot, value_db, value_size, value_query, info_query)


        return render_template('queryresults.html', opt_database=database_options, opt_size=size_options, opt_variables=var_options, opt_queries=query_options, opt_devids=dev_ids_options, opt_devids_check=opt_devids_check, chart=chart)

    data_plot = models.plotting_data_queryresults(data, value_query, dev_ids_options)
    chart = models.generate_interactive_chart_qr(data_plot, value_db, value_size, value_query, info_query)

    return render_template('queryresults.html', opt_database=database_options, opt_size=size_options, opt_variables=var_options, opt_queries=query_options, opt_devids=dev_ids_options, opt_devids_check=dev_ids_options, chart=chart)

@app.route('/performanceresults', methods=['GET', 'POST'])
def performanceresults():
    path_dir_results = "../docker/results"

    if not models.path_res_exist(path_dir_results):
        return render_template('error.html')
        
    results_df = models.existing_query_results(path_dir_results)
    # Tenemos las páginas del xlsx en diferentes variables
    m_data, m_space, m_query_per_size =  models.select_performance_results(results_df,"results")
    database_options = m_data['db_exp'].unique()
    value_db = database_options[0]
    size_options = m_data['size_exp'].unique()
    value_size = [size_options[0]]
    query_options = models.query_options()
    value_query = query_options[0][0]

    if request.method == 'POST':
        value_db = request.form.getlist('opt_database')
        value_size = [int(s) for s in request.form.getlist('opt_size')]
        value_query = request.form.get("q")
        query_options = models.update_query_select_pr(query_options, value_query)
        # Opciones con los identificadores disponibles
        variables_options = models.display_checkbox_vars(m_query_per_size, value_query)
        value_var = [int(v) for v in request.form.getlist('opt_var_check')]

        data_plot = models.plotting_data_performanceresults(m_data, m_space, m_query_per_size, value_db, value_size, value_query, value_var)
        chart = models.generate_interactive_chart_pr(data_plot, value_query, query_options)
        
        return render_template('performanceresults.html', opt_database=database_options, opt_database_check=value_db, opt_size=size_options, opt_size_check=value_size, opt_queries=query_options, opt_variables=variables_options, opt_var_check=value_var, chart=chart)
    
    data_plot = models.plotting_data_performanceresults(m_data, m_space, m_query_per_size, [value_db], value_size, value_query, [])
    chart = models.generate_interactive_chart_pr(data_plot, value_query, query_options)

    return render_template('performanceresults.html', opt_database=database_options, opt_database_check=value_db, opt_size=size_options, opt_size_check=value_size, opt_queries=query_options, chart=chart)

if __name__ == '__main__':
    app.run()