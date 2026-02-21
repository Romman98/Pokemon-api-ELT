from datawarehhouse.database_data import insert_into_tables, drop_tables
from airflow.decorators import task


@task
def transformation():
    schema = "transform"
    table = "pokemon_data"
    data = {}
    drop_tables(schema,table)
    insert_into_tables(schema,table,data)