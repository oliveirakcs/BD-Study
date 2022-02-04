from audioop import add
import psycopg2
import pandas as pd

# Parâmetros de conexão

param = {
    "host": "127.0.0.1",
    "database": "TestDB",
    "user": "postgres",
    "password": "admin"
}

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

'''Retorna todos os dados no formato de Tuplas'''
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

'''Insere dados no BD'''
def insert_data(conn, query):
    
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

'''Adiciona uma coluna vazia ao BD Table '''
def add_column(conn, query):
    
    cursor = conn.cursor()
    try:
        cursor.execute(query)
    except (Exception, psycopg2.DatabaseError) as error:
        print("Error: %s" % error)
        cursor.close()
        return 1

    conn.commit()
    cursor.close()

conn = connect(param)

conn.autocommit = True

'''Tests'''

'''Add an empty column to DB Table'''

# query1 = ''' ALTER TABLE vendas ADD COLUMN telefone VARCHAR '''

# add_column(conn,query1)

'''Get all data from DB'''

# query1 = '''SELECT * FROM vendas'''

# result = get_data(conn, query1)

# print(result)

'''Get column names from DB Table'''

# query3 = ''' SELECT column_name FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME = 'vendas' '''

# result = get_data(conn, query3)

# print(result)

