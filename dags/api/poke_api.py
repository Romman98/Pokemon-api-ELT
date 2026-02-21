import requests
import json
import os
from airflow.decorators import task # pyright: ignore 

# Data needed:
# PokeID:
# Name
# Type
# Stro
#  ng Against
# Weakness
# Gen
# Evolves To
# Evolves From

Generation = 3

@task
def get_pokemon(gen):
    
    list_pokemons = []
    path_file = f"./data/raw/gen/gen_data_{gen}.json"
    
    with open(path_file,'r',encoding="utf-8") as raw_data:
        output = json.load(raw_data)
        for pokemon in output.get("pokemon_species"):
            name = pokemon["name"]
            id = pokemon["url"].split("/")[-2]
            list_pokemons.append((name,id))
            
    for pokemon in list_pokemons:
        
        url = f"https://pokeapi.co/api/v2/pokemon/{pokemon[1]}"
        response = requests.get(url)
        output = response.json()
        path = f"./data/raw/pokemon/gen_{gen}/"

        if not os.path.exists(path):
              os.makedirs(path,exist_ok=True)            
            
        with open(f"{path}/poke_data_{pokemon[1]}.json","w",encoding="utf-8") as json_outfile:
            json.dump(output, json_outfile,indent=2,ensure_ascii=False)
        
@task
def get_gen(id):
    url = f"https://pokeapi.co/api/v2/generation/{id}"
    response = requests.get(url)
    output = response.json()
    path = f"./data/raw/gen"
    
    if not os.path.exists(path):
        os.mkdir(path)
    
    with open(f"{path}/gen_data_{id}.json","w",encoding="utf-8") as json_outfile:
        json.dump(output, json_outfile,indent=2,ensure_ascii=False)
  
def evo_chain_id(id):
    # Used to get the chain id for get_evo        
    url = f"https://pokeapi.co/api/v2/pokemon-species/{id}"
    response = requests.get(url)
    output = response.json()
    evo_id = output.get("evolution_chain")["url"].split("/")[-2]
    return evo_id
      
@task
def get_evo(gen):
    
    chain_id_list = set()
    file_path = f"./data/raw/pokemon/gen_{gen}/"
    
    for file in os.listdir(file_path):
        with open(f"{file_path}/{file}","r",encoding="utf-8") as pokemon:
            id = json.load(pokemon).get("id")
        
        chain_id = evo_chain_id(id)
        chain_id_list.add(chain_id)

    for chain in chain_id_list:
        url = f"https://pokeapi.co/api/v2/evolution-chain/{chain}"
        response = requests.get(url)
        output = response.json()
        path = f"./data/raw/evolution/gen_{gen}"
        if not os.path.exists(path):
            os.makedirs(path, exist_ok=True)
        path_raw = f"{path}/evo_data_{chain}.json"
        
        with open(path_raw,"w",encoding="utf-8") as json_outfile:
            json.dump(output, json_outfile,indent=2,ensure_ascii=False)

def process_evo():
    file_path = f"./data/raw/evolution/"
    
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
                evo3 = evo2_check[0].get("evolves_to",{}).get("name",{})    
        
        evolution_list[id]=[chain,evo1,evo2,evo3]
        path_processed = f"./data/processed/evolution/evo_data_{id}.json"
        with open(path_processed,"w",encoding="utf-8") as json_outfile:
            json.dump(evolution_list, json_outfile,indent=2,ensure_ascii=False)
    
    return evolution_list

@task
def get_types():
    url = "https://pokeapi.co/api/v2/type/"
    
    for type_id in range(1,19):
        response = requests.get(f"{url}/{type_id}")
        output = response.json()
        name = output.get("name")
        path = f"./data/raw/type"
        file = f"{path}/type_{name}.json"
        if not os.path.exists(path):
            os.makedirs(path, exist_ok=True)
        
        with open(file,"w",encoding="utf-8") as json_outfile:
            json.dump(output,json_outfile,indent=2,ensure_ascii=False)
        
if __name__ == "__main__":
    get_gen(Generation)
    get_pokemon(Generation)
    get_evo(Generation)
    get_types()