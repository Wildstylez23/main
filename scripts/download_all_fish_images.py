#!/usr/bin/env python3
"""
Download fish images for all species in our fish database using FishBase parquet via DuckDB.

Output:
- Downloads images to `package/public/images/fish/` (creates dir if missing)
- Writes index JSON to `package/src/data/fish-images.json` mapping species id/name to image file paths

Notes:
- Requires `duckdb`, `pandas`, `requests`, and `tqdm`.
- This script performs network I/O and may take a while for many species.
- If your DuckDB wheel lacks HTTP parquet support, install `pyarrow` and a DuckDB build with HTTP enabled.

Usage:
    pip install duckdb pandas requests tqdm
    python scripts/download_all_fish_images.py

"""
import os
import json
import time
import math
from pathlib import Path
from urllib.parse import urljoin

import duckdb
import pandas as pd
import requests
from tqdm import tqdm
import argparse
import sys

# Config
REPO_ROOT = Path(__file__).resolve().parents[1]
FISH_DB_MODULE = REPO_ROOT / 'package' / 'src' / 'fishDatabase.js'
OUT_DIR = REPO_ROOT / 'package' / 'public' / 'images' / 'fish'
OUT_INDEX = REPO_ROOT / 'package' / 'src' / 'data' / 'fish-images.json'

SPECIES_PARQUET = 'https://fishbase.ropensci.org/fishbase/species.parquet'
PICTURES_PARQUET = 'https://fishbase.ropensci.org/fishbase/pictures.parquet'
BASE_PICTURE_URL = 'https://www.fishbase.se/images/species/'

# Safety limits
REQUESTS_PER_SECOND = 8  # throttle downloads to avoid overloading host
TIME_BETWEEN_REQUESTS = 1.0 / REQUESTS_PER_SECOND


def load_local_species_names():
    """
    Extract canonical scientific names from our local fishDatabase JS file.
    The file exports `fishDatabase` as JS; we will execute a crude parse to extract id/name pairs.
    """
    if not FISH_DB_MODULE.exists():
        raise FileNotFoundError(f"Expected fish database at {FISH_DB_MODULE}")

    text = FISH_DB_MODULE.read_text(encoding='utf-8')
    # Heuristic: the canonical cleaned DB is imported in that file from fishDatabase-cleaned.js
    # but this file still contains an inline JS array in older states. To be robust, try to import
    # the cleaned JS module JSON-ish content by locating `export const fishDatabase = ` and parsing JSON-like.

    start_token = 'export const fishDatabase = '
    idx = text.find(start_token)
    if idx == -1:
        # Fallback: try to read fishDatabase-cleaned.js
        alt = REPO_ROOT / 'package' / 'src' / 'fishDatabase-cleaned.js'
        if alt.exists():
            text = alt.read_text(encoding='utf-8')
            idx = text.find(start_token)
        if idx == -1:
            raise RuntimeError('Could not locate fishDatabase export in repository. Please ensure `package/src/fishDatabase.js` or `fishDatabase-cleaned.js` contains export.')

    arr_text = text[idx + len(start_token):]
    # Attempt to find the terminating semicolon or end-of-file
    # Simple but effective: find the first `];` occurrence after start
    end_idx = arr_text.find('];')
    if end_idx == -1:
        # try very long parse until end
        end_idx = len(arr_text)
    json_like = arr_text[:end_idx+1]

    # The content is JS with double quotes and maybe trailing commas; attempt to convert to valid JSON
    # Remove newlines and trailing commas before closing braces
    # This is a heuristic parser; for large changes it might fail and user should provide a CSV/JSON input instead.
    json_like = json_like.strip()
    # Some files begin with `[` already, ensure it
    if not json_like.startswith('['):
        # try to find first '['
        b = json_like.find('[')
        if b != -1:
            json_like = json_like[b:]

    # Remove JS comments
    import re
    json_clean = re.sub(r'//.*', '', json_like)
    json_clean = re.sub(r'/\*[\s\S]*?\*/', '', json_clean)
    # Remove trailing commas before ] or }
    json_clean = re.sub(r',\s*(\]|\})', r'\1', json_clean)

    try:
        species = json.loads(json_clean)
    except Exception as e:
        # If parsing fails, try to evaluate using a more lenient approach: look for name and id occurrences
        species = []
        name_re = re.compile(r"\"name\"\s*:\s*\"([^\"]+)\"")
        id_re = re.compile(r"\"id\"\s*:\s*\"?([^\",\}]+)\"?")
        # Very rough: split into entries by '},{'
        items = json_like.split('},{')
        for item in items:
            nm = name_re.search(item)
            idi = id_re.search(item)
            if nm:
                name = nm.group(1).strip()
                sid = idi.group(1).strip() if idi else name
                species.append({'id': sid, 'name': name})

    # Normalize output to list of dicts with id and scientific name
    out = []
    for s in species:
        sciname = s.get('scientificName') or s.get('scientific_name') or s.get('name') or s.get('scientific')
        sid = s.get('id') or s.get('SpecCode') or s.get('specCode') or sciname
        if sciname:
            out.append({'id': str(sid), 'scientificName': sciname})

    return out


def query_fishbase_for_images(scientific_names):
    """
    Use DuckDB to query FishBase parquet for PicName(s) per scientific name.
    Returns a DataFrame with ScientificName, SpecCode, PicName, ImageURL

    If the remote parquet URLs are unavailable, instruct the caller to provide
    local parquet files and try again (see script --help).
    """
    # Allow environment overrides or local files by checking availability first
    con = None
    df = None
    use_species = SPECIES_PARQUET
    use_pictures = PICTURES_PARQUET

    # If local files were passed via global variables override, let caller set them
    # (the main() function will override these variables when args are provided)
    # Quick HTTP check only if we're using remote URLs
    if use_species.startswith('http'):
        try:
            head = requests.head(SPECIES_PARQUET, timeout=8)
            if head.status_code != 200:
                print(f"Remote species parquet not available (status {head.status_code}).\nPlease download '{SPECIES_PARQUET}' and '{PICTURES_PARQUET}' and place them locally, or pass local paths using --species/--pictures.")
                return pd.DataFrame()
        except Exception:
            print(f"Could not reach remote FishBase parquet URL {SPECIES_PARQUET}.\nPlease provide local parquet files and re-run the script with --species and --pictures.")
            return pd.DataFrame()

        con = duckdb.connect()
        names_df = pd.DataFrame({'scientific_name': scientific_names})
        con.register('requested_names', names_df)

        # If species parquet is a local file (converted from our JS DB), adapt to its schema
        species_path = Path(use_species)
        pics_path = Path(use_pictures)
        if species_path.exists() and species_path.is_file():
                # load with pandas to normalize columns
                df_species = pd.read_parquet(str(species_path))
                # prefer 'scientificName' or 'scientific_name' or 'name'
                if 'scientificName' in df_species.columns:
                        df_species['ScientificName'] = df_species['scientificName']
                elif 'scientific_name' in df_species.columns:
                        df_species['ScientificName'] = df_species['scientific_name']
                elif 'name' in df_species.columns:
                        df_species['ScientificName'] = df_species['name']
                else:
                        # fallback: try to construct from Genus/Species columns if they exist
                        if 'Genus' in df_species.columns and 'Species' in df_species.columns:
                                df_species['ScientificName'] = (df_species['Genus'].astype(str) + ' ' + df_species['Species'].astype(str)).str.strip()
                        else:
                                df_species['ScientificName'] = df_species.iloc[:, 0].astype(str)

                # SpecCode/id mapping
                if 'id' in df_species.columns:
                        df_species['SpecCode'] = df_species['id']
                elif 'SpecCode' in df_species.columns:
                        df_species['SpecCode'] = df_species['SpecCode']
                else:
                        df_species['SpecCode'] = None

                con.register('matched', df_species[['ScientificName', 'SpecCode']])

                # For pictures: if local pictures parquet exists, read and register; otherwise try remote
                if pics_path.exists() and pics_path.is_file():
                        df_pics = pd.read_parquet(str(pics_path))
                        if 'PicName' in df_pics.columns and 'SpecCode' in df_pics.columns:
                                df_pics['ImageURL'] = df_pics['PicName'].apply(lambda p: BASE_PICTURE_URL + p if isinstance(p, str) else None)
                                con.register('pics', df_pics[['SpecCode', 'PicName', 'ImageURL']])
                        else:
                                con.register('pics', pd.DataFrame(columns=['SpecCode', 'PicName', 'ImageURL']))

                query = f"""
                SELECT DISTINCT m.ScientificName, m.SpecCode, p.PicName, p.ImageURL
                FROM matched m
                JOIN requested_names r ON lower(trim(r.scientific_name)) = lower(trim(m.ScientificName))
                LEFT JOIN pics p ON m.SpecCode = p.SpecCode
                WHERE p.PicName IS NOT NULL
                ORDER BY m.ScientificName;
                """

                try:
                        df = con.execute(query).df()
                except Exception:
                        # If pics table/register wasn't available or remote failed, fallback to local DB images
                        rows = load_images_from_local_db(scientific_names)
                        df = pd.DataFrame(rows)
        else:
                query = f"""
                WITH matched AS (
                    SELECT
                        lower(trim(concat(s.Genus, ' ', s.Species))) AS scientific_lower,
                        concat(s.Genus, ' ', s.Species) AS ScientificName,
                        s.SpecCode
                    FROM read_parquet('{use_species}') s
                ),
                pics AS (
                    SELECT
                        p.SpecCode,
                        p.PicName,
                        concat('{BASE_PICTURE_URL}', p.PicName) AS ImageURL
                    FROM read_parquet('{use_pictures}') p
                    WHERE p.PicName IS NOT NULL
                )
                SELECT DISTINCT m.ScientificName, m.SpecCode, p.PicName, p.ImageURL
                FROM matched m
                JOIN requested_names r ON lower(trim(r.scientific_name)) = m.scientific_lower
                LEFT JOIN pics p USING (SpecCode)
                WHERE p.PicName IS NOT NULL
                ORDER BY m.ScientificName;
                """

                df = con.execute(query).df()
    if con is not None:
        con.close()
    if df is None:
        df = pd.DataFrame()
    return df


    def load_images_from_local_db(scientific_names):
        """
        Fallback: scan local `fishDatabase-cleaned.js` for an `image` field per species.
        Returns a list of dicts with ScientificName, SpecCode (may be None), PicName, ImageURL.
        """
        db_path = REPO_ROOT / 'package' / 'src' / 'fishDatabase-cleaned.js'
        if not db_path.exists():
            return []
        text = db_path.read_text(encoding='utf-8')
        # reuse the simple JSON extraction used elsewhere
        start_token = 'export const fishDatabase = '
        idx = text.find(start_token)
        if idx == -1:
            return []
        arr_text = text[idx + len(start_token):]
        end_idx = arr_text.find('];')
        if end_idx == -1:
            end_idx = len(arr_text)
        json_like = arr_text[:end_idx+1]
        import re
        json_clean = re.sub(r'//.*', '', json_like)
        json_clean = re.sub(r'/\*[\s\S]*?\*/', '', json_clean)
        json_clean = re.sub(r',\s*(\]|\})', r'\1', json_clean)
        try:
            data = json.loads(json_clean)
        except Exception:
            return []

        # build quick lookup by scientific name (exact match)
        lookup = {}
        for item in data:
            sciname = item.get('scientificName') or item.get('scientific_name') or item.get('name')
            if not sciname:
                continue
            img = item.get('image') or item.get('imageUrl') or item.get('image_url')
            lookup[sciname] = img

        rows = []
        for name in scientific_names:
            img = lookup.get(name)
            if not img:
                continue
            # If image looks like a local path (starts with /images or images) keep it
            if isinstance(img, str):
                picname = img.split('/')[-1]
                image_url = img if img.startswith('http') else None
                rows.append({'ScientificName': name, 'SpecCode': None, 'PicName': picname, 'ImageURL': image_url or img})
        return rows


def sanitize_filename(s):
    return ''.join(c for c in s if c.isalnum() or c in (' ', '-', '_')).rstrip().replace(' ', '_')


def download_image(url, out_path):
    try:
        resp = requests.get(url, timeout=20)
        if resp.status_code == 200:
            out_path.parent.mkdir(parents=True, exist_ok=True)
            with open(out_path, 'wb') as f:
                f.write(resp.content)
            return True
        else:
            return False
    except Exception:
        return False


def main():
    parser = argparse.ArgumentParser(description='Download fish images from FishBase for all species in local fishDatabase')
    parser.add_argument('--species', help='Local path to species.parquet (overrides remote)', default=None)
    parser.add_argument('--pictures', help='Local path to pictures.parquet (overrides remote)', default=None)
    parser.add_argument('--names-csv', help='CSV file with scientific names (column "scientificName" or first column)', default=None)
    parser.add_argument('--limit', help='Limit downloads per species (0 = all)', type=int, default=0)
    args = parser.parse_args()

    print('Loading local species list...')
    species = load_local_species_names()
    print(f'Found {len(species)} species locally.')

    # Build scientific names list for query
    scientific_names = [s['scientificName'] for s in species]

    # If user supplied a CSV of names, prefer that list (allows custom subsets)
    if args.names_csv:
        csv_path = Path(args.names_csv)
        if not csv_path.exists():
            print(f"Names CSV not found: {csv_path}")
            return
        try:
            df_names = pd.read_csv(csv_path)
            # Try common column names
            if 'scientificName' in df_names.columns:
                scientific_names = df_names['scientificName'].dropna().astype(str).tolist()
            elif 'scientific_name' in df_names.columns:
                scientific_names = df_names['scientific_name'].dropna().astype(str).tolist()
            elif 'name' in df_names.columns and 'scientificName' not in df_names.columns:
                # sometimes the CSV may use 'name' for scientific name
                scientific_names = df_names['name'].dropna().astype(str).tolist()
            else:
                # fallback: take first column
                first_col = df_names.columns[0]
                scientific_names = df_names[first_col].dropna().astype(str).tolist()
            print(f"Loaded {len(scientific_names)} scientific names from {csv_path}")
        except Exception as e:
            print(f"Failed to read names CSV: {e}")
            return

    # If user provided local parquet paths, override the constants used by query
    global SPECIES_PARQUET, PICTURES_PARQUET
    if args.species:
        SPECIES_PARQUET = str(Path(args.species).resolve())
    if args.pictures:
        PICTURES_PARQUET = str(Path(args.pictures).resolve())

    print('Querying FishBase for pictures (this may take a moment)...')
    df = query_fishbase_for_images(scientific_names)

    if df is None or df.empty:
        print('No pictures found or query failed.')
        return

    # Convert DataFrame to grouped dict by scientific name
    grouped = df.groupby('ScientificName')

    OUT_DIR.mkdir(parents=True, exist_ok=True)
    OUT_INDEX.parent.mkdir(parents=True, exist_ok=True)

    index = {}

    # For each species in local list, try to find matching pictures
    for s in tqdm(species, desc='Species'):
        name = s['scientificName']
        sid = s['id']
        try:
            rows = grouped.get_group(name)
        except KeyError:
            # no pictures in df for this name
            index[sid] = {'scientificName': name, 'images': []}
            continue

        images = []
        for i, row in rows.iterrows():
            pic = row['PicName']
            url = row['ImageURL']
            if not isinstance(pic, str) or not pic.strip():
                continue
            filename = sanitize_filename(f"{sid}_{name}_{pic}")
            # Ensure extension
            if not os.path.splitext(pic)[1]:
                filename = filename + '.jpg'
            out_path = OUT_DIR / filename
            success = False
            if not out_path.exists():
                # throttle
                time.sleep(TIME_BETWEEN_REQUESTS)
                success = download_image(url, out_path)
            else:
                success = True

            if success:
                rel = os.path.relpath(out_path, REPO_ROOT / 'package')
                images.append({'picName': pic, 'url': url, 'path': rel.replace('\\', '/')})
            # Respect --limit if provided
            if args.limit and args.limit > 0 and len(images) >= args.limit:
                break

        index[sid] = {'scientificName': name, 'images': images}

    # Save index
    with open(OUT_INDEX, 'w', encoding='utf-8') as f:
        json.dump(index, f, ensure_ascii=False, indent=2)

    print(f'Download complete. Images saved to {OUT_DIR}. Index written to {OUT_INDEX}')


if __name__ == '__main__':
    main()
