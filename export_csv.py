import pandas as pd
import psycopg2
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




def export_to_csv(db_con, table_name):
    query_template = "SELECT * FROM {}"
    cur = db_con.cursor()
    cur.execute(query_template.format(table_name))
    res = cur.fetchall()
    df = pd.DataFrame(res)
    p = './' + table_name + '.csv'
    df.to_csv(path_or_buf=p, index=False)


if __name__ == "__main__":
    tables = ['location', 'market', 'comodity_price']
    with connect() as db_con:
        for t in tables:
            export_to_csv(db_con, t)


