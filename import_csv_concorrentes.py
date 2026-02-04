#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
PULSE AMAZÔNIA - IMPORTADOR CSV → SUPABASE (CONCORRENTES NACIONAIS)
Lê arquivo coleta-concorrentes-nacionais.csv e insere na tabela concorrentes_nacionais
Estrutura: data_coleta, destino_id, interesse (SEM origens)
"""

import os
import sys
import csv
from datetime import datetime
from supabase import create_client, Client

# ========== CONFIGURAÇÃO SUPABASE ==========
SUPABASE_URL = os.environ.get('SUPABASE_URL')
SUPABASE_KEY = os.environ.get('SUPABASE_KEY')
CSV_PATH = os.environ.get('CSV_PATH', 'coleta-concorrentes-nacionais.csv')

# Validação de variáveis de ambiente
if not SUPABASE_URL or not SUPABASE_KEY:
    print("❌ ERRO: Variáveis SUPABASE_URL e SUPABASE_KEY são obrigatórias")
    sys.exit(1)

# ========== DESTINOS VÁLIDOS (8 CONCORRENTES) ==========
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

# ========== INICIALIZAR CLIENTE SUPABASE ==========
try:
    supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)
    print("✅ Conexão Supabase estabelecida")
except Exception as e:
    print(f"❌ Erro ao conectar Supabase: {e}")
    sys.exit(1)

# ========== FUNÇÃO DE VALIDAÇÃO ==========
def validar_linha(row, linha_num):
    """
    Valida uma linha do CSV antes de inserir no banco
    Retorna: (valido: bool, erro: str)
    """
    # Verificar campos obrigatórios
    if 'data_coleta' not in row or not row['data_coleta']:
        return False, f"Linha {linha_num}: campo 'data_coleta' vazio"
    
    if 'destino_id' not in row or not row['destino_id']:
        return False, f"Linha {linha_num}: campo 'destino_id' vazio"
    
    if 'interesse' not in row or not row['interesse']:
        return False, f"Linha {linha_num}: campo 'interesse' vazio"
    
    # Validar formato de data (YYYY-MM-DD)
    try:
        datetime.strptime(row['data_coleta'], '%Y-%m-%d')
    except ValueError:
        return False, f"Linha {linha_num}: data_coleta inválida (use YYYY-MM-DD): {row['data_coleta']}"
    
    # Validar destino_id
    destino_id = row['destino_id'].strip().lower()
    if destino_id not in CONCORRENTES_VALIDOS:
        return False, f"Linha {linha_num}: destino_id inválido '{destino_id}'. Válidos: {', '.join(sorted(CONCORRENTES_VALIDOS))}"
    
    # Validar interesse (0-100)
    try:
        interesse = int(row['interesse'])
        if not (0 <= interesse <= 100):
            return False, f"Linha {linha_num}: interesse deve estar entre 0-100, recebido: {interesse}"
    except ValueError:
        return False, f"Linha {linha_num}: interesse deve ser número inteiro, recebido: {row['interesse']}"
    
    return True, None

# ========== FUNÇÃO PRINCIPAL ==========
def importar_concorrentes():
    """
    Lê CSV e insere dados na tabela concorrentes_nacionais do Supabase
    """
    print(f"\n🌍 PULSE AMAZÔNIA - IMPORTADOR CONCORRENTES NACIONAIS")
    print("=" * 70)
    print(f"📅 Execução: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"📂 Arquivo: {CSV_PATH}")
    print(f"🔗 Supabase: {SUPABASE_URL[:30]}...")
    print(f"📊 Tabela: concorrentes_nacionais")
    print("=" * 70)
    
    # Verificar se arquivo existe
    if not os.path.exists(CSV_PATH):
        print(f"❌ ERRO: Arquivo {CSV_PATH} não encontrado")
        sys.exit(1)
    
    # Ler CSV
    linhas_processadas = 0
    linhas_validas = 0
    linhas_invalidas = 0
    erros = []
    registros = []
    
    try:
        with open(CSV_PATH, 'r', encoding='utf-8') as arquivo:
            leitor = csv.DictReader(arquivo)
            
            # Validar cabeçalho
            colunas_esperadas = {'data_coleta', 'destino_id', 'interesse'}
            colunas_encontradas = set(leitor.fieldnames) if leitor.fieldnames else set()
            
            if not colunas_esperadas.issubset(colunas_encontradas):
                faltando = colunas_esperadas - colunas_encontradas
                print(f"❌ ERRO: Colunas faltando no CSV: {', '.join(faltando)}")
                print(f"   Colunas encontradas: {', '.join(colunas_encontradas)}")
                sys.exit(1)
            
            print(f"\n✅ Cabeçalho válido: {leitor.fieldnames}")
            print("\n📖 Lendo CSV: coleta-concorrentes-nacionais.csv")
            print("-" * 70)
            
            # Processar linhas
            for idx, row in enumerate(leitor, start=2):  # start=2 porque linha 1 é header
                linhas_processadas += 1
                
                # Validar linha
                valido, erro = validar_linha(row, idx)
                
                if not valido:
                    linhas_invalidas += 1
                    erros.append(erro)
                    print(f"⚠️  {erro}")
                    continue
                
                # Preparar registro para inserção
                registro = {
                    'data_coleta': row['data_coleta'],
                    'destino_id': row['destino_id'].strip().lower(),
                    'interesse': int(row['interesse'])
                }
                
                registros.append(registro)
                linhas_validas += 1
        
        print("-" * 70)
        print(f"\n📊 Linhas processadas: {linhas_processadas}")
        print(f"✅ Linhas válidas: {linhas_validas}")
        print(f"⚠️  Linhas inválidas: {linhas_invalidas}")
        
        if linhas_invalidas > 0:
            print(f"\n⚠️  Erros encontrados: {linhas_invalidas}")
            print("❌ VALIDAÇÃO FALHOU")
            print("\n📋 Detalhes dos erros:")
            for erro in erros:
                print(f"   • {erro}")
            sys.exit(1)
        
        if linhas_validas == 0:
            print("\n⚠️  Nenhuma linha válida para inserir")
            sys.exit(0)
        
        # Inserir no Supabase
        print(f"\n🔄 Inserindo {linhas_validas} registros no Supabase...")
        print("-" * 70)
        
        try:
            # Inserir em lote (batch insert)
            response = supabase.table('concorrentes_nacionais').insert(registros).execute()
            
            # Validar resposta
            if hasattr(response, 'data') and response.data:
                registros_inseridos = len(response.data)
                print(f"✅ {registros_inseridos} registros inseridos com sucesso!")
                
                # Mostrar resumo por destino
                print("\n📊 Resumo por destino:")
                destinos_count = {}
                for reg in registros:
                    dest = reg['destino_id']
                    destinos_count[dest] = destinos_count.get(dest, 0) + 1
                
                for destino in sorted(destinos_count.keys()):
                    count = destinos_count[destino]
                    print(f"   • {destino}: {count} registro(s)")
                
                print("\n" + "=" * 70)
                print("✅ IMPORTAÇÃO CONCLUÍDA COM SUCESSO!")
                print("=" * 70)
                
            else:
                print("⚠️  Resposta do Supabase sem dados")
                print(f"   Response: {response}")
                sys.exit(1)
                
        except Exception as e:
            print(f"\n❌ ERRO ao inserir no Supabase: {e}")
            print(f"   Tipo: {type(e).__name__}")
            
            # Detalhes adicionais se disponíveis
            if hasattr(e, 'message'):
                print(f"   Mensagem: {e.message}")
            if hasattr(e, 'details'):
                print(f"   Detalhes: {e.details}")
            
            sys.exit(1)
            
    except Exception as e:
        print(f"\n❌ ERRO ao processar arquivo CSV: {e}")
        print(f"   Tipo: {type(e).__name__}")
        sys.exit(1)

# ========== EXECUÇÃO ==========
if __name__ == "__main__":
    try:
        importar_concorrentes()
    except KeyboardInterrupt:
        print("\n\n⚠️  Importação cancelada pelo usuário")
        sys.exit(1)
    except Exception as e:
        print(f"\n❌ ERRO FATAL: {e}")
        sys.exit(1)
