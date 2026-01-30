#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
PULSE AMAZÔNIA - IMPORTADOR CSV → SUPABASE (CONCORRENTES NACIONAIS)
====================================================================
Data: 30/01/2026
Desenvolvedor: Liezio Abrantes
Versão: 1.1.0 (ACEITA MÚLTIPLAS DATAS)

OBJETIVO:
Importar dados coletados manualmente do Google Trends (CSV) para o Supabase.
Tabela: concorrentes_nacionais (8 destinos nacionais para comparação IPCR)
Aceita múltiplas coletas (múltiplas datas) no mesmo CSV.
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
CSV_PATH = os.environ.get('CSV_PATH', 'coleta-concorrentes-nacionais.csv')

# Validação de credenciais
if not SUPABASE_URL or not SUPABASE_KEY:
    print("❌ ERRO CRÍTICO: Variáveis SUPABASE_URL e SUPABASE_KEY não configuradas")
    print("💡 Configure no GitHub: Settings → Secrets → Actions")
    sys.exit(1)

# Cliente Supabase (SEM argumento proxy)
try:
    supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)
    print("✅ Conexão Supabase estabelecida")
except Exception as e:
    print(f"❌ ERRO ao conectar Supabase: {str(e)}")
    sys.exit(1)

# Constantes de validação - 8 CONCORRENTES NACIONAIS
DESTINOS_VALIDOS = {
    'manaus', 'sao_luis', 'lencois_maranhenses', 'jalapao',
    'bonito', 'presidente_figueiredo', 'parintins', 'atins'
}

COLUNAS_ESPERADAS = [
    'destino_id', 'data_coleta', 'interesse',
    'origem_1', 'origem_1_pct',
    'origem_2', 'origem_2_pct',
    'origem_3', 'origem_3_pct'
]

# ============================================================================
# VALIDAÇÃO DE DADOS
# ============================================================================

def validar_data(data_str: str) -> Tuple[bool, Optional[str], Optional[str]]:
    """Valida formato de data YYYY-MM-DD."""
    pattern = r'^\d{4}-\d{2}-\d{2}$'
    
    if not re.match(pattern, data_str):
        return False, None, f"Formato inválido: '{data_str}' (esperado: YYYY-MM-DD)"
    
    try:
        ano, mes, dia = data_str.split('-')
        data_obj = datetime(int(ano), int(mes), int(dia))
        data_iso = data_obj.strftime('%Y-%m-%d')
        
        hoje = datetime.now()
        if data_obj > hoje:
            return False, None, f"Data futura: {data_str}"
        
        if data_obj.year < 2020:
            return False, None, f"Data muito antiga: {data_str}"
        
        return True, data_iso, None
        
    except ValueError as e:
        return False, None, f"Data inválida: {data_str} ({str(e)})"


def validar_interesse(valor: str) -> Tuple[bool, Optional[int], Optional[str]]:
    """Valida interesse (0-100)."""
    try:
        valor_int = int(valor)
        
        if valor_int < 0 or valor_int > 100:
            return False, None, f"Interesse fora do range: {valor_int} (esperado: 0-100)"
        
        return True, valor_int, None
        
    except ValueError:
        return False, None, f"Interesse não numérico: '{valor}'"


def validar_percentual(valor: str) -> Tuple[bool, Optional[int], Optional[str]]:
    """Valida percentual (0-100) - retorna INTEGER."""
    try:
        valor_float = float(valor)
        
        if valor_float < 0 or valor_float > 100:
            return False, None, f"Percentual fora do range: {valor_float} (esperado: 0-100)"
        
        # Converte para INTEGER para compatibilidade com Supabase
        return True, int(round(valor_float)), None
        
    except ValueError:
        return False, None, f"Percentual não numérico: '{valor}'"


def validar_linha(linha: Dict[str, str], num_linha: int) -> Tuple[bool, Optional[Dict], List[str]]:
    """Valida uma linha do CSV completamente."""
    erros = []
    
    # Validar destino_id
    destino_id = linha.get('destino_id', '').strip().lower()
    
    if not destino_id:
        erros.append(f"Linha {num_linha}: destino_id vazio")
    elif destino_id not in DESTINOS_VALIDOS:
        erros.append(f"Linha {num_linha}: destino_id inválido '{destino_id}'")
    
    # Validar data_coleta
    data_str = linha.get('data_coleta', '').strip()
    valido_data, data_iso, erro_data = validar_data(data_str)
    
    if not valido_data:
        erros.append(f"Linha {num_linha}: {erro_data}")
    
    # Validar interesse
    interesse_str = linha.get('interesse', '').strip()
    valido_interesse, interesse_val, erro_interesse = validar_interesse(interesse_str)
    
    if not valido_interesse:
        erros.append(f"Linha {num_linha}: {erro_interesse}")
    
    # Validar origens (obrigatório: origem_1 e origem_1_pct)
    origem_1 = linha.get('origem_1', '').strip()
    origem_1_pct_str = linha.get('origem_1_pct', '').strip()
    
    if not origem_1:
        erros.append(f"Linha {num_linha}: origem_1 vazia")
    
    valido_pct1, origem_1_pct, erro_pct1 = validar_percentual(origem_1_pct_str)
    if not valido_pct1:
        erros.append(f"Linha {num_linha}: origem_1_pct {erro_pct1}")
    
    # Validar origens opcionais (2 e 3)
    origem_2 = linha.get('origem_2', '').strip() or None
    origem_2_pct = None
    
    if origem_2:
        origem_2_pct_str = linha.get('origem_2_pct', '').strip()
        valido_pct2, origem_2_pct, erro_pct2 = validar_percentual(origem_2_pct_str)
        if not valido_pct2:
            erros.append(f"Linha {num_linha}: origem_2_pct {erro_pct2}")
    
    origem_3 = linha.get('origem_3', '').strip() or None
    origem_3_pct = None
    
    if origem_3:
        origem_3_pct_str = linha.get('origem_3_pct', '').strip()
        valido_pct3, origem_3_pct, erro_pct3 = validar_percentual(origem_3_pct_str)
        if not valido_pct3:
            erros.append(f"Linha {num_linha}: origem_3_pct {erro_pct3}")
    
    if erros:
        return False, None, erros
    
    dados = {
        'destino_id': destino_id,
        'data_coleta': data_iso,
        'interesse': interesse_val,
        'origem_1': origem_1,
        'origem_1_pct': origem_1_pct,
        'origem_2': origem_2,
        'origem_2_pct': origem_2_pct,
        'origem_3': origem_3,
        'origem_3_pct': origem_3_pct
    }
    
    return True, dados, []


# ============================================================================
# LEITURA E PROCESSAMENTO DO CSV
# ============================================================================

def ler_csv(caminho: str) -> Tuple[bool, List[Dict], List[str]]:
    """Lê e valida CSV completo."""
    erros = []
    dados_validos = []
    
    if not os.path.exists(caminho):
        erros.append(f"Arquivo não encontrado: {caminho}")
        return False, [], erros
    
    print(f"📂 Lendo CSV: {caminho}")
    
    try:
        with open(caminho, 'r', encoding='utf-8') as f:
            sample = f.read(1024)
            f.seek(0)
            
            try:
                dialect = csv.Sniffer().sniff(sample)
            except csv.Error:
                dialect = csv.excel
            
            reader = csv.DictReader(f, dialect=dialect)
            
            colunas_encontradas = reader.fieldnames
            
            if not colunas_encontradas:
                erros.append("CSV vazio ou sem cabeçalho")
                return False, [], erros
            
            colunas_norm = [col.strip().lower() for col in colunas_encontradas]
            faltando = set(COLUNAS_ESPERADAS) - set(colunas_norm)
            
            if faltando:
                erros.append(f"Colunas faltando: {', '.join(faltando)}")
                return False, [], erros
            
            print(f"✅ Cabeçalho válido: {len(colunas_norm)} colunas")
            
            num_linha = 1
            
            for row in reader:
                num_linha += 1
                row_norm = {k.strip().lower(): v for k, v in row.items()}
                valido, dados, erros_linha = validar_linha(row_norm, num_linha)
                
                if valido:
                    dados_validos.append(dados)
                else:
                    erros.extend(erros_linha)
            
            print(f"✅ Linhas processadas: {num_linha - 1}")
            print(f"✅ Linhas válidas: {len(dados_validos)}")
            
            if erros:
                print(f"⚠️  Erros encontrados: {len(erros)}")
            
            # VALIDAÇÃO REMOVIDA: Não exige mais exatamente 8 destinos
            # Permite múltiplas datas no mesmo CSV
            
            # Validar duplicatas (mesmo destino na mesma data)
            chaves_unicas = set()
            for dado in dados_validos:
                chave = (dado['destino_id'], dado['data_coleta'])
                if chave in chaves_unicas:
                    erros.append(f"CRÍTICO: Destino duplicado '{dado['destino_id']}' na data {dado['data_coleta']}")
                chaves_unicas.add(chave)
            
            if erros:
                return False, [], erros
            
            # Mostrar estatísticas
            datas_unicas = set(d['data_coleta'] for d in dados_validos)
            destinos_unicos = set(d['destino_id'] for d in dados_validos)
            print(f"📊 Estatísticas: {len(datas_unicas)} datas | {len(destinos_unicos)} destinos únicos")
            
            return True, dados_validos, []
            
    except Exception as e:
        erros.append(f"Erro ao ler CSV: {str(e)}")
        return False, [], erros


# ============================================================================
# INSERÇÃO NO SUPABASE
# ============================================================================

def inserir_supabase(dados: List[Dict]) -> Tuple[bool, List[str]]:
    """Insere dados no Supabase usando UPSERT (TABELA: concorrentes_nacionais)."""
    erros = []
    
    print(f"\n📤 Inserindo {len(dados)} registros no Supabase (concorrentes_nacionais)...")
    
    try:
        response = supabase.table('concorrentes_nacionais').upsert(
            dados,
            on_conflict='destino_id,data_coleta'
        ).execute()
        
        if hasattr(response, 'data') and response.data:
            registros_inseridos = len(response.data)
            print(f"✅ Supabase: {registros_inseridos} registros inseridos/atualizados")
            return True, []
        else:
            erros.append("Resposta Supabase vazia ou inválida")
            return False, erros
        
    except Exception as e:
        erros.append(f"Erro Supabase: {str(e)}")
        return False, erros


# ============================================================================
# MAIN
# ============================================================================

def main():
    """Execução principal."""
    
    print("\n" + "="*70)
    print("🌍 PULSE AMAZÔNIA - IMPORTADOR CONCORRENTES NACIONAIS")
    print("="*70)
    print(f"📅 Execução: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"📂 Arquivo: {CSV_PATH}")
    print(f"🔗 Supabase: {SUPABASE_URL[:30]}...")
    print(f"📊 Tabela: concorrentes_nacionais")
    print("="*70 + "\n")
    
    sucesso_csv, dados, erros_csv = ler_csv(CSV_PATH)
    
    if not sucesso_csv:
        print("\n" + "="*70)
        print("❌ VALIDAÇÃO FALHOU")
        print("="*70)
        for erro in erros_csv:
            print(f"  • {erro}")
        print("="*70 + "\n")
        sys.exit(1)
    
    print(f"\n✅ CSV validado com sucesso: {len(dados)} registros\n")
    
    sucesso_db, erros_db = inserir_supabase(dados)
    
    if not sucesso_db:
        print("\n" + "="*70)
        print("❌ INSERÇÃO SUPABASE FALHOU")
        print("="*70)
        for erro in erros_db:
            print(f"  • {erro}")
        print("="*70 + "\n")
        sys.exit(1)
    
    print("\n" + "="*70)
    print("✅ IMPORTAÇÃO CONCORRENTES CONCLUÍDA COM SUCESSO")
    print("="*70)
    print(f"📊 Registros processados: {len(dados)}")
    
    # Estatísticas de datas
    datas_unicas = sorted(set(d['data_coleta'] for d in dados))
    print(f"📅 Datas importadas: {len(datas_unicas)}")
    if len(datas_unicas) <= 5:
        for data in datas_unicas:
            count = len([d for d in dados if d['data_coleta'] == data])
            print(f"   • {data}: {count} destinos")
    else:
        print(f"   • Primeira: {datas_unicas[0]}")
        print(f"   • Última: {datas_unicas[-1]}")
    
    print(f"🌍 Destinos nacionais únicos: {len(set(d['destino_id'] for d in dados))}")
    print("="*70 + "\n")
    
    sys.exit(0)


if __name__ == "__main__":
    main()
