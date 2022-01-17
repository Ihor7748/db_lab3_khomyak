import psycopg2
import pandas as pd
import sqlalchemy
import getpass

def connect(initfile='database.ini'):
    with open('./database.ini', 'r') as fd:
        params = fd.read().replace('\n', ' ')
        with_password = False
        
        # check if password is in params
        for p in params.split(' '):
            if p.startswith('password='):
                with_password = True
        # if not ask for password
        if not with_password:
            password = getpass.getpass()
            params += ' password=' + password
    try:
        db_con = psycopg2.connect(params)
    except Exception as error:
        print(error)
        p = params.split(' ')
        for s in p:
            if s.startswith('password='):
                p.remove(s)
        print('unable to connect to:\n', p)
        exit()
    return db_con


def make_db_engine(initfile='database.ini'):
    with open(initfile, 'r') as fd:
        params = fd.read().split('\n')
        with_password = False
        # check if password is in params
        for p in params:
            if p.startswith('password='):
                with_password = True
        # if not ask for password
        if not with_password:
            password = getpass.getpass()
            params += ' password=' + password
        url_template = 'postgresql://{user}:{password}@{host}:{port}/{dbname}'
        for p in params:
            if p.startswith('user'):
                user = p.split('=')[1].strip()[1:-1]
            elif p.startswith('password'):
                password = p.split('=')[1].strip()[1:-1]
            elif p.startswith('host'):
                host = p.split('=')[1].strip()[1:-1]
            elif p.startswith('dbname'):
                dbname = p.split('=')[1].strip()[1:-1]
            elif p.startswith('port'):
                port = p.split('=')[1].strip()
        url = url_template.format(user=user, password=password, host=host, port=port, dbname=dbname)
    return sqlalchemy.create_engine(url)

def divide_dataframe(filename, dataframe, tables, cols, csv_cols):
    pass

def load_data(filename, table_names, engine, csv_cols, table_cols=None):
    df_full = pd.read_csv(filename, usecols=csv_cols[0])
    df1 = df_full[csv_cols[1]].drop_duplicates()
    df2 = df_full[csv_cols[2]].drop_duplicates(subset=['mkt_id'])
    df3 = df_full[csv_cols[3]].drop_duplicates(subset=['cm_name', 'mkt_id', 'mp_year', 'mp_month'])
    df2.insert(2, 'location_id', df2['adm1_id'])
    for i in df2.index:
        for j in df1.index:
            if df2.at[i, 'adm1_id'] == df1.at[j, 'adm1_id'] and df2.at[i, 'adm0_name'] == df2.at[j, 'adm0_name']:
                df2.at[i, 'location_id'] = j
    df2 = df2[['mkt_id', 'mkt_name', 'location_id', 'pt_name']]
    df1 = df1[['adm1_name', 'adm0_name']]
    if table_cols:
        df1.columns = table_cols[0] if table_cols[0] else df1.columns
        df2.columns = table_cols[1] if table_cols[1] else df2.columns
        df3.columns = table_cols[2] if table_cols[2] else df3.columns
    print('loading table location')
    df1.to_sql(table_names[0], engine, if_exists='append', index='location_id')
    print('loading table market')
    df2.to_sql(table_names[1], engine, if_exists='append', index=False)
    print('loading table comodity_price')
    df3.to_sql(table_names[2], engine, if_exists='append', index=False)


if __name__ == '__main__':

    csv_file = 'global_food_prices_10k_random_sample.csv'
    connection_file = 'database.ini'
    tables = ['location', 'market', 'comodity_price']
    cols1 = ['locality_name', 'country_name']
    cols2 = ['market_id', 'market_name', 'location_id', 'market_type']
    cols3 = ['comodity', 'market_id', 'year', 'month', 'price', 'currency', 'units']
    csv_cols = [['adm1_id', 'adm1_name', 'adm0_name', 'mkt_id', 'mkt_name', 
                 'pt_name','cm_name', 'mp_year', 'mp_month', 'mp_price', 'cur_name', 'um_name'],
                ['adm1_id', 'adm1_name', 'adm0_name'], 
                ['mkt_id', 'mkt_name','adm1_id', 'adm0_name', 'pt_name'], 
                ['cm_name', 'mkt_id', 'mp_year', 'mp_month', 'mp_price', 'cur_name', 'um_name']]

    

    with connect(connection_file) as db_con:
        db_engine = make_db_engine(connection_file)
        print('engine done')
        load_data(csv_file, tables, db_engine, csv_cols, [cols1, cols2, cols3])








