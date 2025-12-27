import json
import re
import pandas as pd

def convert_js_to_parquet(input_file, output_file):
    print(f"Lezen van {input_file}...")
    
    try:
        with open(input_file, 'r', encoding='utf-8') as f:
            content = f.read()
            
        # 1. Schoon de JavaScript syntax op naar geldige JSON
        # Verwijder 'export const fishDatabase ='
        json_str = re.sub(r'export const fishDatabase =\s*', '', content)
        # Verwijder de puntkomma aan het eind en witruimte
        json_str = json_str.strip().rstrip(';')
        
        # 2. Laad de data
        data = json.loads(json_str)
        
        # 3. Zet om naar Pandas DataFrame
        df = pd.DataFrame(data)
        
        # Optioneel: Omdat je geneste data hebt (zoals temperature: {min, max}),
        # kan Parquet dit opslaan, maar soms is het handiger om complexe kolommen 
        # even om te zetten naar strings als je simpele viewers gebruikt. 
        # Voor nu laten we het zo (PyArrow kan geneste data aan).
        
        # 4. Sla op als Parquet
        print("Converteren naar Parquet...")
        df.to_parquet(output_file, engine='pyarrow', compression='snappy', index=False)
        
        print(f"Succes! Bestand opgeslagen als: {output_file}")
        
        # Even checken hoe groot het is en de eerste regels tonen
        print("-" * 30)
        df_check = pd.read_parquet(output_file)
        print(df_check[['name', 'scientificName', 'habitat']].head())
        print("-" * 30)

    except Exception as e:
        print(f"Er ging iets mis: {e}")

# Voer de functie uit
convert_js_to_parquet('fishDatabase-cleaned.js', 'vissen.parquet')