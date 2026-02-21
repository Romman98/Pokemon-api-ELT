from airflow.providers.postgres.hooks.postgres import PostgresHook
from psycopg2.extras import RealDictCursor


def get_conn_cursor():
    hook = PostgresHook(postgres_conn_id="postgres_db_yt_elt", database="elt_db")
    conn = hook.get_conn()
    cur = conn.cursor(cursor_factory=RealDictCursor)
    return conn, cur


def close_conn_cursor(conn, cur):
    conn.commit()
    cur.close()
    conn.close()
    
def create_schema(schema):
    conn, cur = get_conn_cursor()

    schema_sql = f"CREATE SCHEMA IF NOT EXISTS {schema};"

    cur.execute(schema_sql)
    conn.commit()
    close_conn_cursor(conn, cur)

def create_table(schema):
    conn, cur = get_conn_cursor()

    if schema == "processing":
        pokemon_table_sql = f"""
                CREATE TABLE IF NOT EXISTS {schema}.pokemon (
                    "PokeID" VARCHAR(4) PRIMARY KEY NOT NULL,
                    "Name" VARCHAR(30),
                    "Type" VARCHAR(20),
                    "Gen" VARCHAR(2)
                );
            """
        evolution_table_sql = f"""
                CREATE TABLE IF NOT EXISTS {schema}.evolution (
                    "EvoID" VARCHAR(5) PRIMARY KEY NOT NULL,
                    "Chain" VARCHAR(5),
                    "Base" VARCHAR(50),
                    "First_Evolution" VARCHAR(50),
                    "Second_Evolution" VARCHAR(50)
                );
            """
        type_table_sql = f"""
                CREATE TABLE IF NOT EXISTS {schema}.type (
                    "Type" VARCHAR(50) PRIMARY KEY NOT NULL,
                    "Double_Damage_From" JSONB,
                    "Double_Damage_To" JSONB,
                    "Half_Damage_From" JSONB,
                    "Half_Damage_To" JSONB
                );
            """         

    cur.execute(pokemon_table_sql)
    cur.execute(evolution_table_sql)
    cur.execute(type_table_sql)

    conn.commit()

    close_conn_cursor(conn, cur)