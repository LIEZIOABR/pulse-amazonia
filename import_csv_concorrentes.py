#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os
import sys
import csv
from datetime import datetime
from supabase import create_client

SUPABASE_URL = os.environ.get('SUPABASE_URL')
SUPABASE_KEY = os.environ.get('SUPABASE_KEY')
CSV_PATH = os.environ.get('CSV_PATH', 'coleta-concorrentes-nacionais.csv')

if not SUPABASE_URL or not SUPABASE_KEY:
    print("ERROR: SUPABASE_URL and SUPABASE_KEY are required")
    sys.exit(1)

CONCORRENTES_VALIDOS = {
    'manaus',
    'sao_luis',
    'lencois_maranhenses',
    'jalapao',
    'bonito',
    'presidente_figueiredo',
    'parintins',
    'atins'
}

try:
    supabase = create_client(SUPABASE_URL, SUPABASE_KEY)
    print("SUCCESS: Supabase connection established")
except Exception as e:
    print(f"ERROR: Failed to connect to Supabase: {e}")
    sys.exit(1)


def validar_linha(row, linha_num):
    if not row.get('data_coleta'):
        return False, f"Line {linha_num}: 'data_coleta' empty"

    if not row.get('destino_id'):
        return False, f"Line {linha_num}: 'destino_id' empty"

    if not row.get('interesse'):
        return False, f"Line {linha_num}: 'interesse' empty"

    try:
        datetime.strptime(row['data_coleta'], '%Y-%m-%d')
    except ValueError:
        return False, f"Line {linha_num}: invalid date"

    destino_id = row['destino_id'].strip().lower()
    if destino_id not in CONCORRENTES_VALIDOS:
        return False, f"Line {linha_num}: invalid destino_id '{destino_id}'"

    try:
        interesse = int(row['interesse'])
        if not (0 <= interesse <= 100):
            return False, f"Line {linha_num}: interesse must be 0-100"
    except ValueError:
        return False, f"Line {linha_num}: interesse must be integer"

    return True, None


def importar_concorrentes():
    print("\nPULSE AMAZONIA - NATIONAL COMPETITORS UPSERT")
    print("=" * 70)
    print(f"Execution: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"File: {CSV_PATH}")
    print("=" * 70)

    if not os.path.exists(CSV_PATH):
        print(f"ERROR: File {CSV_PATH} not found")
        sys.exit(1)

    registros = []

    with open(CSV_PATH, 'r', encoding='utf-8') as arquivo:
        leitor = csv.DictReader(arquivo)

        for idx, row in enumerate(leitor, start=2):
            valido, erro = validar_linha(row, idx)

            if not valido:
                print(f"WARNING: {erro}")
                continue

            registros.append({
                'data_coleta': row['data_coleta'],
                'destino_id': row['destino_id'].strip().lower(),
                'interesse': int(row['interesse'])
            })

    if not registros:
        print("No valid records to process")
        sys.exit(0)

    print(f"\nUPSERTING {len(registros)} records...")
    print("-" * 70)

    try:
        response = (
            supabase
            .table('concorrentes_nacionais')
            .upsert(
                registros,
                on_conflict='destino_id,data_coleta'
            )
            .execute()
        )

        print("SUCCESS: Records upserted successfully!")
        print("=" * 70)
        print("IMPORT COMPLETED")
        print("=" * 70)

    except Exception as e:
        print(f"\nERROR during UPSERT: {e}")
        sys.exit(1)


if __name__ == "__main__":
    importar_concorrentes()
