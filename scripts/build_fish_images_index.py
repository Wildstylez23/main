#!/usr/bin/env python3
"""
Build an index JSON mapping species slug -> available image filenames.

Writes: package/src/data/fish-images.json

Uses `package/src/data/scientific_names.csv` if present to map slugs to species IDs and scientific names.
"""
import os
import csv
import json
import argparse
import unicodedata
import re


def slugify(name: str) -> str:
    s = unicodedata.normalize('NFKD', name)
    s = s.encode('ascii', 'ignore').decode('ascii')
    s = s.lower()
    s = re.sub(r"[^a-z0-9]+", '-', s)
    s = re.sub(r'-{2,}', '-', s)
    s = s.strip('-')
    return s


def load_scientific_names(csv_path):
    mapping = {}
    if not os.path.exists(csv_path):
        return mapping
    with open(csv_path, newline='', encoding='utf-8') as fh:
        reader = csv.DictReader(fh)
        for r in reader:
            sid = r.get('id') or r.get('species_id') or r.get('SpeciesID')
            name = r.get('scientificName') or r.get('scientific_name') or r.get('ScientificName')
            if not name:
                continue
            s = slugify(name)
            mapping[s] = {'id': sid, 'scientific_name': name}
    return mapping


def best_slug_for(base, known_slugs):
    # Try full base first, then progressively strip trailing -parts
    candidate = base
    parts = base.split('-')
    for i in range(len(parts), 0, -1):
        cand = '-'.join(parts[:i])
        if cand in known_slugs:
            return cand
    # fallback: return original base
    return candidate


def build_index(images_dir, names_csv=None, out_path=None):
    files = [f for f in os.listdir(images_dir) if os.path.isfile(os.path.join(images_dir, f))]
    mapping = load_scientific_names(names_csv) if names_csv else {}
    known_slugs = set(mapping.keys())

    index = {}
    total_files = 0
    for fn in files:
        total_files += 1
        name, ext = os.path.splitext(fn)
        if ext.lower() not in ('.png', '.jpg', '.jpeg', '.webp', '.svg'):
            continue
        # attempt to find slug by stripping trailing numeric/size parts
        slug_candidate = best_slug_for(name, known_slugs)
        # ensure entry exists
        entry = index.setdefault(slug_candidate, {'files': []})
        entry['files'].append(fn)

    # enrich with scientific names when available
    matched = 0
    for slug, info in index.items():
        meta = mapping.get(slug)
        if meta:
            info['species_id'] = meta.get('id')
            info['scientific_name'] = meta.get('scientific_name')
            matched += 1
        else:
            info['species_id'] = None
            info['scientific_name'] = None

    out = {
        'generated_from': os.path.abspath(images_dir),
        'file_count': total_files,
        'species_count': len(index),
        'matched_to_names_csv': matched,
        'images': index,
    }

    if out_path:
        out_dir = os.path.dirname(out_path)
        if out_dir and not os.path.exists(out_dir):
            os.makedirs(out_dir, exist_ok=True)
        with open(out_path, 'w', encoding='utf-8') as fh:
            json.dump(out, fh, ensure_ascii=False, indent=2)

    return out


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--images-dir', default='package/public/images/fish')
    parser.add_argument('--names-csv', default='package/src/data/scientific_names.csv')
    parser.add_argument('--out', default='package/src/data/fish-images.json')
    args = parser.parse_args()

    images_dir = os.path.abspath(args.images_dir)
    if not os.path.exists(images_dir):
        print('Images directory does not exist:', images_dir)
        raise SystemExit(2)

    out = build_index(images_dir, args.names_csv, args.out)
    print('Wrote:', os.path.abspath(args.out))
    print('Files scanned:', out['file_count'])
    print('Species (unique slugs):', out['species_count'])
    print('Matched to names CSV:', out['matched_to_names_csv'])


if __name__ == '__main__':
    main()
