#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os
import sys
import csv
from datetime import datetime, date
from supabase import create_client

SUPABASE_URL = os.environ.get("SUPABASE_URL")
SUPABASE_KEY = os.environ.get("SUPABASE_KEY")
CSV_PATH = os.environ.get("CSV_PATH", "coleta-concorrentes-nacionais.csv")

# Segurança: por padrão NÃO força data.
# Se quiser forçar a data do dia sem editar o gerador do CSV, defina: FORCE_TODAY=1
FORCE_TODAY = os.environ.get("FORCE_TODAY", "0") == "1"

# Segurança: por padrão, exige que a data do CSV seja HOJE.
# Se quiser permitir datas diferentes (não recomendado), defina: REQUIRE_TODAY=0
REQUIRE_TODAY = os.environ.get("REQUIRE_TODAY", "1") == "1"

CONCORRENTES_VALIDOS = {
    "manaus",
    "sao_luis",
    "lencois_maranhenses",
    "jalapao",
    "bonito",
    "presidente_figueiredo",
    "parintins",
    "atins",
}

def die(msg: str, code: int = 1):
    print(msg)
    sys.exit(code)

if not SUPABASE_URL or not SUPABASE_KEY:
    die("ERROR: SUPABASE_URL and SUPABASE_KEY are required")

try:
    supabase = create_client(SUPABASE_URL, SUPABASE_KEY)
    print("SUCCESS: Supabase connection established")
except Exception as e:
    die(f"ERROR: Failed to connect to Supabase: {e}")

def validar_linha(row, linha_num):
    if not row.get("data_coleta"):
        return False, f"Line {linha_num}: 'data_coleta' empty"
    if not row.get("destino_id"):
        return False, f"Line {linha_num}: 'destino_id' empty"
    if row.get("interesse") in (None, ""):
        return False, f"Line {linha_num}: 'interesse' empty"

    try:
        datetime.strptime(row["data_coleta"], "%Y-%m-%d")
    except ValueError:
        return False, f"Line {linha_num}: invalid date '{row['data_coleta']}' (expected YYYY-MM-DD)"

    destino_id = row["destino_id"].strip().lower()
    if destino_id not in CONCORRENTES_VALIDOS:
        return False, f"Line {linha_num}: invalid destino_id '{destino_id}'"

    try:
        interesse = int(row["interesse"])
        if not (0 <= interesse <= 100):
            return False, f"Line {linha_num}: interesse must be 0-100"
    except ValueError:
        return False, f"Line {linha_num}: interesse must be integer"

    return True, None

def importar_concorrentes():
    print("\nPULSE AMAZONIA - NATIONAL COMPETITORS IMPORT (SAFE)")
    print("=" * 70)
    print(f"Execution: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"File: {CSV_PATH}")
    print("Table: concorrentes_nacionais")
    print(f"FORCE_TODAY: {FORCE_TODAY} | REQUIRE_TODAY: {REQUIRE_TODAY}")
    print("=" * 70)

    if not os.path.exists(CSV_PATH):
        die(f"ERROR: File {CSV_PATH} not found")

    registros = []
    datas_encontradas = set()

    with open(CSV_PATH, "r", encoding="utf-8") as arquivo:
        leitor = csv.DictReader(arquivo)

        colunas_esperadas = {"data_coleta", "destino_id", "interesse"}
        colunas_encontradas = set(leitor.fieldnames or [])
        if not colunas_esperadas.issubset(colunas_encontradas):
            faltando = colunas_esperadas - colunas_encontradas
            die(f"ERROR: Missing columns: {', '.join(sorted(faltando))}")

        for idx, row in enumerate(leitor, start=2):
            valido, erro = validar_linha(row, idx)
            if not valido:
                die(f"ERROR: {erro}")

            data_csv = row["data_coleta"].strip()
            datas_encontradas.add(data_csv)

            registro = {
                "data_coleta": data_csv,
                "destino_id": row["destino_id"].strip().lower(),
                "interesse": int(row["interesse"]),
            }
            registros.append(registro)

    if not registros:
        die("No valid records to process", code=0)

    # Log das datas do CSV (diagnóstico decisivo)
    datas_ordenadas = sorted(datas_encontradas)
    print(f"\nCSV dates found: {datas_ordenadas}")

    hoje = date.today().strftime("%Y-%m-%d")

    if REQUIRE_TODAY and (datas_encontradas != {hoje}):
        die(
            f"ERROR: CSV date is not today.\n"
            f"Expected ONLY: {hoje}\n"
            f"Found: {datas_ordenadas}\n"
            f"Fix: update the CSV generator to write today's date, or run with FORCE_TODAY=1 (last resort)."
        )

    if FORCE_TODAY:
        for r in registros:
            r["data_coleta"] = hoje
        print(f"FORCE_TODAY applied -> all records will be written as {hoje}")

    print(f"\nUPSERTING {len(registros)} records...")
    print("-" * 70)

    try:
        # Upsert seguro para manter histórico por (destino_id, data_coleta)
        resp = (
            supabase
            .table("concorrentes_nacionais")
            .upsert(registros, on_conflict="destino_id,data_coleta")
            .execute()
        )
        print("SUCCESS: Records upserted successfully!")
        print("=" * 70)
        print("IMPORT COMPLETED")
        print("=" * 70)
    except Exception as e:
        die(f"ERROR during UPSERT: {e}")

if __name__ == "__main__":
    importar_concorrentes()
