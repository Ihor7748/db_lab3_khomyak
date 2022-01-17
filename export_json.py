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




def export_to_json(db_con, table):
    query_template = "SELECT * FROM {}"
    cur = db_con.cursor()
    cur.execute(query_template.format(table))
    res = cur.fetchall()
    df = pd.DataFrame(res)
    return df.to_json()



if __name__ == "__main__":
    tables = ['location', 'market', 'comodity_price']
    file = './database.json'
    with open(file, 'w') as fd:
        fd.write('')
    with connect() as db_con:
        for t in range(len(tables)):
            single_t = '{}}}' if t == len(tables) - 1 else '{},'
            res = export_to_json(db_con, tables[t])
            with open(file, 'a') as fd:
                table = f'"{tables[t]}":{res}'
                fd.write(single_t.format(table))
