# /// script
# requires-python = ">=3.13"
# dependencies = ['polars', 'pmed-tools@git+ssh://git@github.com/siensmetrica/pmed-tools@009a761afb076920029aaf28903df544fc9b724e']
# ///

import json
import zipfile
import argparse

from io import TextIOWrapper
from pathlib import Path

import polars as pl

from pmed_tools.dataset import open_dataset_reader


def extract_rows(zf: zipfile.ZipFile) -> list[dict]:
    rows = []
    for fname in zf.namelist():
        if not fname.endswith('.json'):
            continue

        category = Path(fname).stem
        with zf.open(fname) as f:
            items = json.load(TextIOWrapper(f, encoding='utf-8'))

        for group in items:
            for row in group['rankings']:
                rows.append({
                    'category': category,
                    'title': row['publication'],
                    'h5_index': row['h5_index'],
                    'h5_median': row['h5_median'],
                    'number_rank': row['number_rank'],
                })

    return rows


def main(args: argparse.Namespace):
    args.output.parent.mkdir(parents=True, exist_ok=True)

    with open_dataset_reader() as reader:

        with zipfile.ZipFile(args.input) as zf:
            rows = extract_rows(zf)

        frame = pl.from_dicts(rows).lazy()
        frame = (
            frame.join(
                (
                    reader
                    .ctx
                    .journals
                    .scan()
                    .select(['iso_abv', 'issn', 'title'])
                    .unique(keep='last')
                ),
                on='title',
                how='left'
            ).sort(['category', 'title', 'issn'])
        ).collect()

        frame.write_parquet(args.output)

        print(f'Wrote {len(frame)} rows to {args.output}')


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Import top journal rankings from zip')
    parser.add_argument(
        '--input', type=Path, help='Path to zip file with JSON data', default='top_journals.json.zip'
    )
    parser.add_argument(
        '--output', type=Path, help='Path to write the parquet output', default='top_journals.parquet'
    )
    args = parser.parse_args()

    main(args)
