import psycopg


def get_db(query):
    try:
        conn = psycopg.connect(
            host="localhost",
            user="romman",
            password="password",
            dbname="rommandb",
            port=5432)
        cur = conn.cursor()
        output = cur.execute(query)
        cur.close()
        conn.close()
    
        return output
    except Exception as e:
        return f"query failed: {e}"
    
def initialize_db():

    conn = psycopg.connect(
        host="localhost",
        user="romman",
        password="password",
        dbname="rommandb",
        port=5432)
    
    cur = conn.cursor()
    
    cur.execute(""" CREATE TABLE IF NOT EXISTS person (
        id INT PRIMARY KEY,
        name VARCHAR(255),
        age INT,
        gender CHAR
    );""")
    
    conn.commit()
    cur.close()
    conn.close()

def sample_data():
    conn = psycopg.connect(
        host="localhost",
        user="romman",
        password="password",
        dbname="rommandb",
        port=5432)
    cur = conn.cursor()
    cur.execute("""INSERT INTO person (id, name, age, gender) VALUES 
            (1, 'Ahmad',28,'M'),
            (2, 'Ali',64,'M'),
            (3, 'Farah',21,'F'),
            (4, 'Sarah',53,'F')
             """)
    conn.commit()

    cur.close()
    conn.close()

def empty_database():
    conn = psycopg.connect(
        host="localhost",
        user="romman",
        password="password",
        dbname="rommandb",
        port=5432)
    cur = conn.cursor()
    
    cur.execute("drop table person;")
    conn.commit()
    cur.close()
    conn.close()