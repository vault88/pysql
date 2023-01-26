import psycopg2
from psycopg2 import Error
data='database="netology_db", user="postgres",password="test1234"'

def create_tables(conn):
    with conn.cursor() as cur:
        cur.execute('''DROP TABLE phones; DROP TABLE clients''')
        cur.execute('''CREATE TABLE IF NOT EXISTS clients(
        id SERIAL PRIMARY KEY,
        first_name VARCHAR(40) NOT NULL,
        last_name VARCHAR(40) NOT NULL,
        email VARCHAR(40) UNIQUE NOT NULL);
        CREATE TABLE IF NOT EXISTS phones(
        id SERIAL PRIMARY KEY,
        client_id INTEGER NOT NULL REFERENCES clients(id),
        phone VARCHAR(11) UNIQUE);
        ''')
    conn.commit()

def add_client(conn, first_name, last_name, email, phones=None):
    with conn.cursor() as cur:
        cur.execute('''
        INSERT INTO clients(first_name, last_name, email) 
        VALUES(%s, %s, %s);
        ''', (first_name, last_name, email,))
        if phones != None:
            cur.execute('''
            INSERT INTO phones(client_id, phone)
            SELECT id, %s FROM clients 
            WHERE id = (SELECT MAX(id) FROM clients); 
            ''', (phones,))
    conn.commit()

def add_phone(conn, client_id, phone):
    with conn.cursor() as cur:
        cur.execute('''
        INSERT INTO phones(client_id, phone)
        VALUES(%s, %s);
        ''', (client_id, phone,))
    conn.commit()

def change_client(conn, client_id, first_name=None, last_name=None, email=None, phones=None):
    with conn.cursor() as cur:
        if first_name == None:
            insert_query = '''SELECT * FROM clients WHERE id=%s;'''
            cur.execute(insert_query,(client_id,))
            first_name = cur.fetchone()[1]
        if last_name == None:
            insert_query = '''SELECT * FROM clients WHERE id=%s;'''
            cur.execute(insert_query,(client_id,))
            last_name = cur.fetchone()[2]
        if email == None:
            insert_query = '''SELECT * FROM clients WHERE id=%s;'''
            cur.execute(insert_query,(client_id,))
            email = cur.fetchone()[3]
        cur.execute('''
        UPDATE clients
        SET first_name=%s, last_name=%s, email=%s
        WHERE id = %s;
        ''', (first_name, last_name, email, client_id,))
        if phones != None:
            cur.execute('''
            INSERT INTO phones(client_id, phone)
            VALUES(%s,%s);
            ''', (client_id, phones,))
    conn.commit()

def delete_phone(conn, client_id, phone):
    with conn.cursor() as cur:
        insert_query = '''DELETE FROM phones WHERE client_id=%s AND phone=%s;'''
        cur.execute(insert_query, (client_id, str(phone),))
    conn.commit()

def delete_client(conn, client_id):
    with conn.cursor() as cur:
        insert_query = '''DELETE FROM phones WHERE client_id=%s;'''
        cur.execute(insert_query, (client_id,))
        insert_query = '''DELETE FROM clients WHERE id=%s;'''
        cur.execute(insert_query, (client_id,))
    conn.commit()

def find_client(conn, first_name=None, last_name=None, email=None, phone=None):
    with conn.cursor() as cur:
        if phone != None:
            insert_query = '''
            SELECT c.first_name, c.last_name, c.email, answer.phone FROM clients c
            LEFT JOIN (select * from phones p where phone=%s) as answer
            ON c.id=answer.client_id
            WHERE c.first_name=%s or null 
            AND c.last_name=%s or null 
            AND c.email=%s or null
            order by c.first_name;
            '''
        else:
            insert_query = '''
            SELECT c.first_name, c.last_name, c.email, answer.phone FROM clients c
            JOIN (select * from phones p where phone=%s or phone is not null) as answer
            ON c.id=answer.client_id
            WHERE c.first_name=%s or null 
            AND c.last_name=%s or null 
            AND c.email=%s or null
            order by c.first_name;
            '''
        cur.execute(insert_query, (phone, first_name, last_name, email ))
        print('\nРезультаты поиска клиента:\n',cur.fetchall()) 

def show_table(table,conn):
    with conn.cursor() as cur:
        insert_query = f"SELECT * FROM {table}"
        cur.execute(insert_query)
        data = cur.fetchall()
        print('\nТаблица ', table,':')
        for row in data:
            print(row)

with psycopg2.connect(database="netology_db", user="postgres",password="test1234") as conn:
    try:
        print("Соединение с PostgreSQL открыто")
        create_tables(conn)
        add_client(conn, 'test1', 'qwe', 'email1')
        add_client(conn, 'test2', 'qwe', 'email2', 123231)
        add_client(conn, 'test3', 'qwe', 'email3', 121313231)
        add_phone(conn, 2, 1234)
        change_client(conn, client_id=2, first_name='test4')
        delete_phone(conn, 2, 123231)
        delete_client(conn, 2)
        show_table('clients',conn)
        show_table('phones',conn)
        find_client(conn,first_name='test3', last_name='qwe')
    except (Exception, Error) as error:
        print('Ошибка при работе с PostgreSQL', error)
conn.close()
print('\nСоединение с PostgreSQL закрыто')
