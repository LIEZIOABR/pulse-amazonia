#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
PULSE AMAZÔNIA - IMPORTADOR CSV → SUPABASE
============================================
Versão: 1.2.0 (RESET MARABÁ)
Atualização: Substituição braganca → maraba
"""

import os
import sys
import csv
import re
from datetime import datetime
from typing import List, Dict, Optional, Tuple
from supabase import create_client, Client

# ============================================================================
# CONFIGURAÇÃO
# ============================================================================

SUPABASE_URL = os.environ.get('SUPABASE_URL')
SUPABASE_KEY = os.environ.get('SUPABASE_KEY')
CSV_PATH = os.environ.get('CSV_PATH', 'coleta-trends-para.csv')

if not SUPABASE_URL or not SUPABASE_KEY:
    print("❌ ERRO CRÍTICO: Variáveis SUPABASE_URL e SUPABASE_KEY não configuradas")
    sys.exit(1)

try:
    supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)
    print("✅ Conexão Supabase estabelecida")
except Exception as e:
    print(f"❌ ERRO ao conectar Supabase: {str(e)}")
    sys.exit(1)

# ============================================================================
# DESTINOS VÁLIDOS (ATUALIZADO)
# ============================================================================

DESTINOS_VALIDOS = {
    'belem',
    'alter_do_chao',
    'ilha_marajo',
    'santarem',
    'salinopolis',
    'soure',
    'salvaterra',
    'mosqueiro',
    'maraba',           # <- atualizado
    'monte_alegre',
    'algodoal',
    'obidos',
    'cameta',
    'parauapebas',
    'castanhal'
}

COLUNAS_ESPERADAS = [
    'destino_id', 'data_coleta', 'interesse',
    'origem_1', 'origem_1_pct',
    'origem_2', 'origem_2_pct',
    'origem_3', 'origem_3_pct'
]

# ============================================================================
# VALIDAÇÃO
# ============================================================================

def validar_data(data_str: str) -> Tuple[bool, Optional[str], Optional[str]]:
    pattern = r'^\d{4}-\d{2}-\d{2}$'
    if not re.match(pattern, data_str):
        return False, None, f"Formato inválido: '{data_str}'"
    try:
        data_obj = datetime.strptime(data_str, "%Y-%m-%d")
        if data_obj > datetime.now():
            return False, None, f"Data futura: {data_str}"
        if data_obj.year < 2020:
            return False, None, f"Data muito antiga: {data_str}"
        return True, data_str, None
    except Exception as e:
        return False, None, f"Data inválida: {data_str} ({str(e)})"

def validar_interesse(valor: str) -> Tuple[bool, Optional[int], Optional[str]]:
    try:
        valor_int = int(valor)
        if 0 <= valor_int <= 100:
            return True, valor_int, None
        return False, None, f"Interesse fora do range: {valor_int}"
    except:
        return False, None, f"Interesse inválido: '{valor}'"

def validar_percentual(valor: str) -> Tuple[bool, Optional[int], Optional[str]]:
    try:
        valor_int = int(round(float(valor)))
        if 0 <= valor_int <= 100:
            return True, valor_int, None
        return False, None, f"Percentual fora do range: {valor}"
    except:
        return False, None, f"Percentual inválido: '{valor}'"

def validar_linha(linha: Dict[str, str], num_linha: int):
    erros = []

    destino_id = linha.get('destino_id','').strip().lower()
    if destino_id not in DESTINOS_VALIDOS:
        erros.append(f"Linha {num_linha}: destino_id inválido '{destino_id}'")

    ok_data, data_iso, erro_data = validar_data(linha.get('data_coleta','').strip())
    if not ok_data:
        erros.append(f"Linha {num_linha}: {erro_data}")

    ok_int, interesse, erro_int = validar_interesse(linha.get('interesse','').strip())
    if not ok_int:
        erros.append(f"Linha {num_linha}: {erro_int}")

    ok_pct1, origem_1_pct, erro_pct1 = validar_percentual(linha.get('origem_1_pct','').strip())
    if not ok_pct1:
        erros.append(f"Linha {num_linha}: {erro_pct1}")

    if erros:
        return False, None, erros

    dados = {
        'destino_id': destino_id,
        'data_coleta': data_iso,
        'interesse': interesse,
        'origem_1': linha.get('origem_1'),
        'origem_1_pct': origem_1_pct,
        'origem_2': linha.get('origem_2'),
        'origem_2_pct': linha.get('origem_2_pct'),
        'origem_3': linha.get('origem_3'),
        'origem_3_pct': linha.get('origem_3_pct')
    }

    return True, dados, []

# ============================================================================
# LEITURA CSV
# ============================================================================

def ler_csv(caminho: str):
    if not os.path.exists(caminho):
        print(f"❌ Arquivo não encontrado: {caminho}")
        sys.exit(1)

    dados_validos = []
    erros = []

    with open(caminho, 'r', encoding='utf-8') as f:
        reader = csv.DictReader(f)

        for i, row in enumerate(reader, start=2):
            row_norm = {k.strip().lower(): v for k,v in row.items()}
            valido, dados, erros_linha = validar_linha(row_norm, i)

            if valido:
                dados_validos.append(dados)
            else:
                erros.extend(erros_linha)

    if erros:
        print("❌ VALIDAÇÃO FALHOU:")
        for e in erros:
            print(" -", e)
        sys.exit(1)

    return dados_validos

# ============================================================================
# INSERÇÃO
# ============================================================================

def inserir_supabase(dados: List[Dict]):
    try:
        response = supabase.table('pulse_amazonia').upsert(
            dados,
            on_conflict='destino_id,data_coleta'
        ).execute()

        print(f"✅ {len(dados)} registros inseridos/atualizados com sucesso")
    except Exception as e:
        print(f"❌ Erro ao inserir no Supabase: {e}")
        sys.exit(1)

# ============================================================================
# MAIN
# ============================================================================

def main():
    print("\n🌴 PULSE AMAZÔNIA - IMPORTAÇÃO DEFINITIVA")
    print("="*60)
    print(f"📅 Execução: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"📂 CSV: {CSV_PATH}")
    print("="*60)

    dados = ler_csv(CSV_PATH)
    inserir_supabase(dados)

    print("\n✅ IMPORTAÇÃO CONCLUÍDA COM SUCESSO")
    print("="*60)
    sys.exit(0)

if __name__ == "__main__":
    main()
