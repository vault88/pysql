import psycopg2
from psycopg2 import Error

def create_tables(cur):
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


def add_client(cur, first_name, last_name, email, phones=None):
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

def add_phone(cur, client_id, phone):
    cur.execute('''
    INSERT INTO phones(client_id, phone)
    VALUES(%s, %s);
    ''', (client_id, phone,))


def change_client(cur, client_id, first_name=None, last_name=None, email=None, phones=None):
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


def delete_phone(cur, client_id, phone):
    insert_query = '''DELETE FROM phones WHERE client_id=%s AND phone=%s;'''
    cur.execute(insert_query, (client_id, str(phone),))


def delete_client(cur, client_id):
    insert_query = '''DELETE FROM phones WHERE client_id=%s;'''
    cur.execute(insert_query, (client_id,))
    insert_query = '''DELETE FROM clients WHERE id=%s;'''
    cur.execute(insert_query, (client_id,))


def find_client(cur, first_name=None, last_name=None, email=None, phone=None):
    if phone == None:
        insert_query = '''
        SELECT c.first_name, c.last_name, c.email, answer.phone FROM clients c
        LEFT JOIN (select * from phones p) as answer
        ON c.id=answer.client_id
        WHERE (c.first_name=%s OR %s IS NULL) 
        AND (c.last_name=%s OR %s IS NULL) 
        AND (c.email=%s OR %s IS NULL);
        '''
        cur.execute(insert_query, (first_name, first_name, last_name, last_name, email, email, ))
    else:
        insert_query = '''
        SELECT c.first_name, c.last_name, c.email, answer.phone FROM clients c
        RIGHT JOIN (select * from phones p WHERE phone=%s) as answer
        ON c.id=answer.client_id
        WHERE (c.first_name=%s OR %s IS NULL) 
        AND (c.last_name=%s OR %s IS NULL) 
        AND (c.email=%s OR %s IS NULL)
        order by c.first_name;
        '''
        cur.execute(insert_query, (phone, first_name, first_name, last_name, last_name, email, email, ))
    print('\nРезультаты поиска клиента:\n',cur.fetchall()) 


def show_table(table,cur):
    insert_query = f"SELECT * FROM {table}"
    cur.execute(insert_query)
    data = cur.fetchall()
    print('\nТаблица ', table,':')
    for row in data:
        print(row)



if __name__ == '__main__':
    with psycopg2.connect(database="netology_db", user="postgres",password="test1234") as conn:
        try:
            with conn.cursor() as cur:
                print("Соединение с PostgreSQL открыто")
                create_tables(cur)
                add_client(cur, 'test1', 'qwe', 'email1')
                add_client(cur, 'test2', 'qwe', 'email2', 123231)
                add_client(cur, 'test3', 'qwdade', 'email3', 121313231)
                add_client(cur, 'test5', 'qwe', 'email6', 61616)
                add_phone(cur, 2, 1234)
                change_client(cur, client_id=2, first_name='test4')
                delete_phone(cur, 2, 123231)
                delete_client(cur, 2)
                show_table('clients',cur)
                show_table('phones',cur)
                find_client(cur, last_name='qwe',phone='61616')
                find_client(cur, first_name='test3')
        except (Exception, Error) as error:
            print('Ошибка при работе с PostgreSQL', error)
    if conn:
        cur.close()
        conn.close()
    print('\nСоединение с PostgreSQL закрыто')
