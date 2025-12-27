import json
import re
import pandas as pd
from pathlib import Path

def laad_vis_database(bestandsnaam):
    """
    Leest het JS/JSON bestand en zet het om naar een Pandas DataFrame.
    """
    try:
        with open(bestandsnaam, 'r', encoding='utf-8') as f:
            content = f.read()

        # Stap 1: Schoon de JS syntax op zodat we pure JSON overhouden
        # We verwijderen 'export const fishDatabase =' en de puntkomma op het eind
        json_str = re.sub(r'export const fishDatabase =\s*', '', content)
        json_str = json_str.strip().rstrip(';')

        # Stap 2: Laad de JSON data
        data = json.loads(json_str)

        # Stap 3: Zet om naar DataFrame voor makkelijk zoeken
        df = pd.DataFrame(data)
        return df
    except FileNotFoundError:
        print(f"Fout: Kan bestand '{bestandsnaam}' niet vinden.")
        return None
    except Exception as e:
        print(f"Fout bij het laden van de database: {e}")
        return None

def zoek_vis(df, zoekterm):
    """
    Zoekt een vis op wetenschappelijke naam of gewone naam (case-insensitive).
    """
    if df is None:
        return

    # Zoek in zowel 'name' als 'scientificName'
    # We maken alles lowercase voor de vergelijking
    mask = (
        df['scientificName'].str.lower().str.contains(zoekterm.lower()) | 
        df['name'].str.lower().str.contains(zoekterm.lower())
    )
    
    resultaten = df[mask]
    
    if len(resultaten) == 0:
        print(f"Geen vissen gevonden voor: '{zoekterm}'")
    else:
        print(f"--- {len(resultaten)} Resultaten gevonden voor '{zoekterm}' ---")
        for index, row in resultaten.iterrows():
            print(f"\nNaam: {row.get('name','-')}")
            print(f"Wetenschappelijke naam: {row.get('scientificName','-')}")
            print(f"Afbeelding: {row.get('image','-')}")
            print(f"Beschrijving: {row.get('description','-')}")
            print("-" * 30)

if __name__ == '__main__':
    # Use the cleaned fish database from the package
    repo_root = Path(__file__).resolve().parents[1]
    db_path = repo_root / 'package' / 'src' / 'fishDatabase-cleaned.js'

    print(f"Loading database from: {db_path}")
    df_vissen = laad_vis_database(str(db_path))

    if df_vissen is not None:
        print("Database geladen! Je kunt nu zoeken.\n")
        zoek_vis(df_vissen, "Carassius")
        zoek_vis(df_vissen, "Guppy")
    else:
        print('Database kon niet geladen worden.')
