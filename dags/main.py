from airflow import DAG # pyright: ignore 
import pendulum # pyright: ignore
from datetime import datetime, timedelta
from api.poke_api import get_gen, get_pokemon, get_evo, get_types


local_tz = pendulum.timezone("Asia/Amman")
generation = 3
default_args = {
    "owner" : "dataengineers",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "email": "romman.ahmad@outlook.com",
    # "retries": 1,
    # "retry_delay", timedelta(minute=5),
    "max_active_runs": 1,
    "dagrun_timeout": timedelta(hours=1),
    "start_date": datetime(2025, 1, 1, tzinfo=local_tz),
    # "end_date": datetime(2025,1,1 tzinfo=local_tz),
}

with DAG (
    dag_id = 'produce_json',
    default_args=default_args,
    description = "Extracts PokeAPI and produces .JSON files",
    schedule="0 9 * * *",
    catchup=False,
) as dag_produce:
    Generation = get_gen(generation)
    Pokemon = get_pokemon(generation)
    Evolution = get_evo(generation)
    Types = get_types()
    
    Generation >> Pokemon >> Evolution >> Types # type: ignore