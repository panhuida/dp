


def call_sql_files(sql_list_file_name, **pg_params):
    print(sql_list_file_name)
    # print(pg_params['pg_host'])
    # print(sql_params['mapping_name'])    
    print(pg_params.items())
    # print(sql_params.items())



# sql_params = {'sql_file': sql_file, 'sql_list_file': sql_list_file, 'mapping_name': mapping_name,
#                    'parameter_values': sql_params}

sql_params = {'sql_file': None, 'sql_list_file': 'etl/hour_sql_list', 'mapping_name': None,
                   'parameter_values': "begin_date='2023-03-14',end_date='2023-03-15'" }
sql_list_file = sql_params['sql_lis  t_file']

pg_host = '127.0.0.1'
pg_port = 5432
pg_dbname = 'bigdata'
pg_user = 'bigdata'
pg_password = 'Abcd1201'
pg_params = {'pg_host': pg_host, "pg_port": pg_port, 'pg_dbname': pg_dbname, 'pg_user': pg_user,
            'pg_password': pg_password} 

call_sql_files(sql_list_file, **pg_params, **sql_params)
# call_sql_files(sql_list_file, pg_params,  sql_params)