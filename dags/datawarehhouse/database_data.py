# from airflow.providers.postgres.hooks.postgres import PostgresHook
# from psycopg2.extras import RealDictCursor
from datawarehhouse.database_management import get_conn_cursor, close_conn_cursor, create_schema, create_table
import os, json

import logging

logger = logging.getLogger(__name__)

create_schema("processing")
create_table("processing")
create_schema("transform")
create_table("transform")

def file_opener(path):
    with open(path,'r',encoding="utf-8") as raw_data:
        data = json.load(raw_data)
        return data

def drop_tables(schema,table):
    conn, cur = get_conn_cursor()
    cur.execute(f"TRUNCATE TABLE {schema}.{table}")
    close_conn_cursor(conn, cur)

def insert_into_tables(schema,table,data):
    conn, cur = get_conn_cursor()
    if schema == "processing":
        if table == "pokemon":
            cur.execute(
                f"""
                INSERT INTO processing.pokemon ("PokeID","Name","Type","Gen")
                VALUES (%(poke_id)s,%(name)s,%(type)s,%(gen)s);
                """, data
            )
            logger.info(f"Updated row with Data: {data}")
            
        elif table == "evolution":
            cur.execute(
                f"""
                INSERT INTO processing.evolution ("EvoID","Base","First_Evolution","Second_Evolution")
                VALUES (%(evo_id)s,%(base)s,%(first_evo)s,%(second_evo)s);
                """, data
            )
            logger.info(f"Updated row with Data: {data}")
            
        elif table == "type":
            cur.execute(
                f"""
                INSERT INTO processing.type ("Type","Double_Damage_From","Double_Damage_To","Half_Damage_From","Half_Damage_To")
                VALUES (%(name)s,%(double_damage_from)s,%(double_damage_to)s,%(half_damage_from)s,%(half_damage_to)s);
                """, data
            )
            logger.info(f"Updated row with Data: {data}")
            
            
    elif schema == "transform":
        cur.execute("""INSERT INTO transform.pokemon_data (
    "PokeID",
    "Name",
    "Gen",
    "Type",
    "Base",
    "NextEvolution",
    "StrongAgainst",
    "WeakAgainst"
)
SELECT
    p."PokeID",
    p."Name",
    p."Gen",
    p."Type",
    e."Base",

    CASE
        WHEN p."Name" = e."Base" THEN e."First_Evolution"
        WHEN p."Name" = e."First_Evolution" THEN e."Second_Evolution"
        ELSE NULL
    END AS "NextEvolution",

    tf."StrongAgainst",
    tf."WeakAgainst"

FROM processing.pokemon p

LEFT JOIN processing.evolution e
    ON p."Name" = e."Base"
    OR p."Name" = e."First_Evolution"
    OR p."Name" = e."Second_Evolution"

LEFT JOIN (
    SELECT
        t."Type",

        (
            SELECT jsonb_agg(DISTINCT value)
            FROM (
                SELECT jsonb_array_elements(t."Double_Damage_To") AS value
                UNION
                SELECT jsonb_array_elements(t."Half_Damage_From")
            ) s
        ) AS "StrongAgainst",

        (
            SELECT jsonb_agg(DISTINCT value)
            FROM (
                SELECT jsonb_array_elements(t."Double_Damage_From") AS value
                UNION
                SELECT jsonb_array_elements(t."Half_Damage_To")
            ) s
        ) AS "WeakAgainst"

    FROM processing.type t
) tf
ON p."Type" = tf."Type";""")
    
    close_conn_cursor(conn, cur)




    
