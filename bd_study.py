import logging
import traceback
import psycopg2
import pandas as pd
from sqlalchemy import create_engine, engine
from psycopg2.extras import LoggingConnection
import io
from datetime import date


data = str(date.today())+'.txt'

# Parâmetros de conexão

param = {
    "host": "127.0.0.1",
    "database": "TestDB",
    "user": "postgres",
    "password": "admin"
}

engine = create_engine(
    'postgresql+psycopg2://postgres:admin@127.0.0.1:5432/TestDB')

logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger("loggerinformation")

# TESTE

log_capture_string = io.StringIO()
ch = logging.StreamHandler(log_capture_string)
ch.setLevel(logging.DEBUG)

formatter = logging.Formatter(
    '%(asctime)s - %(name)s - %(levelname)s - %(message)s')
ch.setFormatter(formatter)

logger.addHandler(ch)

# TESTE

'''Conexão com o BD'''


def connect(param):

    conn = None
    try:
        print('Conectando no servidor...')
        conn = psycopg2.connect(**param)
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
        sys.exit(1)

    print('Conectado com sucesso!')

    return conn


'''Converte o SELECT em um DF Pandas'''


def pg_to_df(conn, query, column_names):

    cursor = conn.cursor()
    try:
        cursor.execute(query)
    except (Exception, psycopg2.DatabaseError) as error:
        print("Error: %s" % error)
        cursor.close()
        return 1

    tupples = cursor.fetchall()
    cursor.close()

    df = pd.DataFrame(tupples, columns=column_names)
    return df


''' Input das queries no BD '''


def get_data(conn, query):

    cursor = conn.cursor()
    try:
        cursor.execute(query)
    except (Exception, psycopg2.DatabaseError) as error:
        print("Error: %s" % error)
        cursor.close()
        return 1

    tupples = cursor.fetchall()
    cursor.close()

    return tupples


'''Adiciona dados do BD '''


def add_data(conn, query):

    cursor = conn.cursor()
    try:
        cursor.execute(query)
    except (Exception, psycopg2.DatabaseError) as error:
        print("Error: %s" % error)
        cursor.close()
        return 1

    conn.commit()
    cursor.close()


'''Remove dados do BD '''


def remove_data(conn, query, id):

    cursor = conn.cursor()

    try:
        cursor.execute(query, (id,))
    except (Exception, psycopg2.DatabaseError) as error:
        print("Error: %s" % error)
        cursor.close()
        return 1

    conn.commit()
    cursor.close()


#conn = connect(param)
# conn.inicialize(logger)

conn = psycopg2.connect(connection_factory=LoggingConnection, **param)

conn.initialize(logger)


conn.autocommit = True

'''Tests'''

''' Checa dados da coluna '''

query1 = '''SELECT * FROM vendas'''

result = get_data(conn, query1)

# print(result)

''' Adiciona um usuário na tabela do BD '''

# query2 = ''' INSERT INTO vendas (product_name, price, type, year) VALUES ('Moto',20000, 'CG', 2021 ) '''

# add_data(conn,query2)

'''Adiciona uma coluna vazia no BD'''

# query1 = ''' ALTER TABLE vendas ADD COLUMN telefone VARCHAR '''

# add_column(conn,query1)

'''Retorna os nomes das colunas'''

query3 = ''' SELECT column_name FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME = 'vendas' '''

result = get_data(conn, query3)

lista_pg = []
lista_parquet = []

for item in result:

    lista_pg.append(item[0])

arquivo = pd.read_csv('dados.csv', sep=";")

arquivo.to_parquet('dados.parquet', index=False)

df = pd.read_parquet('dados.parquet', engine="fastparquet", index=False)

dw_table = 'vendas'

for item in df.columns:
    lista_parquet.append(item)

if (len(set(lista_pg).intersection(lista_parquet))) == len(lista_parquet) and len(lista_pg):

    df.to_sql(dw_table, engine, if_exists="append", index=False)
    print("A")

else:
    df.to_sql(dw_table, engine, if_exists="replace", index=False)
    print('B')

log_contents = log_capture_string.getvalue()
log_capture_string.close()

with open(data, 'a+') as f:
    f.write(log_contents.lower())
    f.write('\n')