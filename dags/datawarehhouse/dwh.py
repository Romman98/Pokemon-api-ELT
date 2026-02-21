import os, json
from datawarehhouse.database_data import insert_into_tables, file_opener, drop_tables
from airflow.decorators import task

@task
def insert_data_pokemon():
    schema = "processing"
    table = "pokemon"
    drop_tables(schema,table)
    file_path=f"./data/raw/pokemon/gen_3/"
    
    for file in os.listdir(file_path):
        data = file_opener(f"{file_path}/{file}")
        PokeID = data.get("id")
        Name = data.get("name")
        Type = data["types"][0]["type"].get("name")
        Gen = 3
        loaded_data = {
            "poke_id":PokeID,
            "name" : Name,
            "type" : Type,
            "gen" : Gen
        }

        insert_into_tables(schema,table,loaded_data)
        
        path_processed = f"./data/processed/pokemon/poke_data_{PokeID}.json"
        with open(path_processed,"w",encoding="utf-8") as json_outfile:
            json.dump(loaded_data, json_outfile,indent=2,ensure_ascii=False)     

@task        
def insert_data_evolution():
    schema = "processing"
    table = "evolution"
    drop_tables(schema,table)
    file_path = f"./data/raw/evolution/gen_3"
    
    for file in os.listdir(file_path):
        try:
            id = file.split(".")[0].split("_")[-1]
            with open(f"{file_path}/{file}",'r',encoding='utf-8') as raw_data:
                evo_raw = json.load(raw_data)
        except Exception as e:
            return f"File {file} was not opened during processing due to: {e}"

        evolution_list = {}
        chain = evo_raw.get("chain",{})
        evo1 = chain.get("species",{}).get("name")
        evo2 = None
        evo3 = None
        
        evo1_check = chain.get("evolves_to",{})
        
        if evo1_check:
            evo2 = evo1_check[0].get("species",{}).get("name",{})

            evo2_check = evo1_check[0].get("evolves_to",{})
            if evo2_check:
                evo3 = evo2_check[0].get("species",{}).get("name",{})    
        
        evolution_list[id]=[evo1,evo2,evo3]
        evolution_list_data = {
            "evo_id": id,
            "base" : evolution_list[id][0],
            "first_evo" : evolution_list[id][1],
            "second_evo" : evolution_list[id][2]
        }
        insert_into_tables("processing","evolution",evolution_list_data)
        
        path_processed = f"./data/processed/evolution/evo_data_{id}.json"
        
        with open(path_processed,"w",encoding="utf-8") as json_outfile:
            json.dump(evolution_list, json_outfile,indent=2,ensure_ascii=False)        

@task
def insert_into_types():
    schema = "processing"
    table = "type"
    drop_tables(schema,table)
    file_path = f"./data/raw/type"

    for file in os.listdir(file_path):
        double_damage_from=[]
        double_damage_to=[] 
        half_damage_from=[]
        half_damage_to=[]
        with open(f"{file_path}/{file}",'r',encoding='utf-8') as raw_data:
                type_raw = json.load(raw_data)
                name = type_raw.get("name")
                for type in type_raw.get("damage_relations").get("double_damage_from"):
                    double_damage_from.append(type.get("name"))
                for type in type_raw["damage_relations"]["double_damage_to"]:
                    double_damage_to.append(type.get("name"))
                for type in type_raw["damage_relations"]["half_damage_from"]:
                    half_damage_from.append(type.get("name"))
                for type in type_raw["damage_relations"]["half_damage_to"]:
                    half_damage_to.append(type.get("name"))
        data = {
            "name" : name,
            "double_damage_from":json.dumps(double_damage_from),
            "double_damage_to":json.dumps(double_damage_to),
            "half_damage_from":json.dumps(half_damage_from),
            "half_damage_to":json.dumps(half_damage_to),
        }
        insert_into_tables("processing","type",data)
        
        path_processed = f"./data/processed/type/type_{name}.json"
        
        with open(path_processed,"w",encoding="utf-8") as json_outfile:
            json.dump(data, json_outfile,indent=2,ensure_ascii=False) 