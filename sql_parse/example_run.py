# Get all table infos in rootdir
folder_frequency = 'weekly'
rootdir = ('/Users/andrea/Documents/redmodo/etl/redshift/process/'
            + folder_frequency
            + '/')
basic_info = dict()
for subdir, dirs, files in os.walk(rootdir):
    for file in files:
        file_name = os.path.join(subdir, file)
        if file_name.split('.')[1] == 'sql':
            print(file_name)
            psf = parse_sql_files(file_name)
            basic_info[file_name] = (psf.get_basic_created_tables())
            basic_info[file_name].update(psf.get_basic_insert_tables())
            basic_info[file_name].update(psf.get_basic_tables_and_columns())


# brake down dictionary to componets created tables
pd.DataFrame([[i, str(j)]
                for i in basic_info.keys()
                for j in list(basic_info[i]['created_tables'])],
             columns = ['query_path'
             , 'created_table']).to_csv('./created_tables_from_'
                                           + folder_frequency
                                           + '_queries.csv')
# brake down dictionary to componets insert tables
pd.DataFrame([[i, str(j)]
                for i in basic_info.keys()
                for j in list(basic_info[i]['insert_tables'])],
             columns = ['query_path'
             , 'insert_table']).to_csv('./insert_tables_from_'
                                          + folder_frequency
                                          + '_queries.csv')
# brake down dictionary to componets used tables and columns
pd.DataFrame(data = [[i, j, k]
                        for i in basic_info.keys()
                        for j in list(basic_info[i]['tables'])
                        for k in list(basic_info[i]['tables'][j])],
            columns = ['query_path',
                        'table',
                        'column']).to_csv('./tables_columns_from_'
                                               + folder_frequency
                                               + '_queries.csv')
