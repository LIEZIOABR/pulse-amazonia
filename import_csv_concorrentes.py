#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os
import sys
import csv
from datetime import datetime
from supabase import create_client, Client

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
    if 'data_coleta' not in row or not row['data_coleta']:
        return False, f"Line {linha_num}: 'data_coleta' field is empty"
    
    if 'destino_id' not in row or not row['destino_id']:
        return False, f"Line {linha_num}: 'destino_id' field is empty"
    
    if 'interesse' not in row or not row['interesse']:
        return False, f"Line {linha_num}: 'interesse' field is empty"
    
    try:
        datetime.strptime(row['data_coleta'], '%Y-%m-%d')
    except ValueError:
        return False, f"Line {linha_num}: invalid data_coleta: {row['data_coleta']}"
    
    destino_id = row['destino_id'].strip().lower()
    if destino_id not in CONCORRENTES_VALIDOS:
        return False, f"Line {linha_num}: invalid destino_id '{destino_id}'"
    
    try:
        interesse = int(row['interesse'])
        if not (0 <= interesse <= 100):
            return False, f"Line {linha_num}: interesse must be 0-100, got: {interesse}"
    except ValueError:
        return False, f"Line {linha_num}: interesse must be integer"
    
    return True, None

def importar_concorrentes():
    print("\nPULSE AMAZONIA - NATIONAL COMPETITORS IMPORTER")
    print("=" * 70)
    print(f"Execution: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"File: {CSV_PATH}")
    print(f"Table: concorrentes_nacionais")
    print("=" * 70)
    
    if not os.path.exists(CSV_PATH):
        print(f"ERROR: File {CSV_PATH} not found")
        sys.exit(1)
    
    linhas_processadas = 0
    linhas_validas = 0
    linhas_invalidas = 0
    erros = []
    registros = []
    
    try:
        with open(CSV_PATH, 'r', encoding='utf-8') as arquivo:
            leitor = csv.DictReader(arquivo)
            
            colunas_esperadas = {'data_coleta', 'destino_id', 'interesse'}
            colunas_encontradas = set(leitor.fieldnames) if leitor.fieldnames else set()
            
            if not colunas_esperadas.issubset(colunas_encontradas):
                faltando = colunas_esperadas - colunas_encontradas
                print(f"ERROR: Missing columns: {', '.join(faltando)}")
                sys.exit(1)
            
            print(f"\nValid header: {leitor.fieldnames}")
            print("\nReading CSV...")
            print("-" * 70)
            
            for idx, row in enumerate(leitor, start=2):
                linhas_processadas += 1
                
                valido, erro = validar_linha(row, idx)
                
                if not valido:
                    linhas_invalidas += 1
                    erros.append(erro)
                    print(f"WARNING: {erro}")
                    continue
                
                registro = {
                    'data_coleta': row['data_coleta'],
                    'destino_id': row['destino_id'].strip().lower(),
                    'interesse': int(row['interesse'])
                }
                
                registros.append(registro)
                linhas_validas += 1
        
        print("-" * 70)
        print(f"\nProcessed: {linhas_processadas} | Valid: {linhas_validas} | Invalid: {linhas_invalidas}")
        
        if linhas_invalidas > 0:
            print("\nVALIDATION FAILED - Errors:")
            for erro in erros:
                print(f"   - {erro}")
            sys.exit(1)
        
        if linhas_validas == 0:
            print("\nNo valid lines to insert")
            sys.exit(0)
        
        print(f"\nInserting {linhas_validas} records into Supabase...")
        print("-" * 70)
        
        try:
            response = supabase.table('concorrentes_nacionais').insert(registros).execute()
            
            if hasattr(response, 'data') and response.data:
                registros_inseridos = len(response.data)
                print(f"SUCCESS: {registros_inseridos} records inserted!")
                
                print("\nSummary by destination:")
                destinos_count = {}
                for reg in registros:
                    dest = reg['destino_id']
                    destinos_count[dest] = destinos_count.get(dest, 0) + 1
                
                for destino in sorted(destinos_count.keys()):
                    print(f"   - {destino}: {destinos_count[destino]} record(s)")
                
                print("\n" + "=" * 70)
                print("IMPORT COMPLETED SUCCESSFULLY!")
                print("=" * 70)
            else:
                print("WARNING: Supabase response without data")
                sys.exit(1)
                
        except Exception as e:
            print(f"\nERROR inserting into Supabase: {e}")
            sys.exit(1)
            
    except Exception as e:
        print(f"\nERROR processing CSV: {e}")
        sys.exit(1)

if __name__ == "__main__":
    try:
        importar_concorrentes()
    except KeyboardInterrupt:
        print("\n\nImport cancelled by user")
        sys.exit(1)
    except Exception as e:
        print(f"\nFATAL ERROR: {e}")
        sys.exit(1)