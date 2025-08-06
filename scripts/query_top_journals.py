# /// script
# requires-python = ">=3.12"
# dependencies = ['polars']
# ///

import sys
import argparse

from pathlib import Path

import polars as pl


def main(args: argparse.Namespace) -> None:
    if not args.input.exists():
        sys.exit(f'Parquet file not found: {args.input_path}')

    lf = pl.scan_parquet(args.input)

    if args.columns:
        lf = lf.select(args.columns)

    df = lf.collect()

    pl.Config.set_tbl_rows(len(df))
    pl.Config.set_tbl_cols(len(df.columns))
    pl.Config.set_tbl_width_chars(200)

    print(df)

    print(f'Table @ "{args.input}" row count: {len(df)}')


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Query top_journals parquet file')
    parser.add_argument('--input', type=Path, help='Parquet top journals file', default='top_journals.parquet')
    parser.add_argument('columns', nargs='*', help='Optional column names to select')
    args = parser.parse_args()

    main(args)

