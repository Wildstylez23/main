import json
import re
from pathlib import Path
import csv


def load_fish_db(js_path: Path):
    text = js_path.read_text(encoding='utf-8')
    start_token = 'export const fishDatabase = '
    idx = text.find(start_token)
    if idx == -1:
        # try alternative file name or content
        raise RuntimeError(f'Could not find fishDatabase export in {js_path}')
    arr_text = text[idx + len(start_token):]
    end_idx = arr_text.find('];')
    if end_idx == -1:
        end_idx = len(arr_text)
    json_like = arr_text[:end_idx+1]

    # Clean JS-ish content to valid JSON
    json_like = re.sub(r'//.*', '', json_like)
    json_like = re.sub(r'/\*[\s\S]*?\*/', '', json_like)
    json_clean = re.sub(r',\s*(\]|\})', r'\1', json_like)

    data = json.loads(json_clean)
    return data


def main():
    repo_root = Path(__file__).resolve().parents[1]
    js_file = repo_root / 'package' / 'src' / 'fishDatabase-cleaned.js'
    out_csv = repo_root / 'package' / 'src' / 'data' / 'scientific_names.csv'
    out_csv.parent.mkdir(parents=True, exist_ok=True)

    print(f'Loading {js_file}...')
    data = load_fish_db(js_file)
    print(f'Parsed {len(data)} species')

    with out_csv.open('w', newline='', encoding='utf-8') as fh:
        writer = csv.writer(fh)
        writer.writerow(['id', 'scientificName'])
        for item in data:
            sid = item.get('id') or ''
            sciname = item.get('scientificName') or item.get('scientific_name') or item.get('name') or ''
            writer.writerow([sid, sciname])

    print(f'Wrote CSV to {out_csv}')


if __name__ == '__main__':
    main()
