from pytrends.request import TrendReq
from pytrends.exceptions import TooManyRequestsError
import pandas as pd
from datetime import datetime, timedelta, timezone
import unicodedata
import csv
import time
import random
import sys
import os
import requests

# ==============================
# CONFIGURAÇÕES GERAIS
# ==============================

MAX_RETRIES = 5
BASE_SLEEP = 4
BACKOFF_FACTOR = 2
TIMEFRAME = 'now 7-d'
GEO = 'BR'

pytrends = TrendReq(hl='pt-BR', tz=180)

# ==============================
# SUPABASE
# ==============================

SUPABASE_URL = os.environ.get("SUPABASE_URL", "")
SUPABASE_KEY = os.environ.get("SUPABASE_SERVICE_ROLE_KEY", "")

# ==============================
# FUNÇÃO DE DATA (BRASIL)
# ==============================

def data_brasil():
    return (datetime.now(timezone.utc) - timedelta(hours=3)).strftime('%Y-%m-%d')

# ==============================
# DESTINOS ANCORA (9)
# Cesta de intencao - 4 termos por destino
# Formula: indice_final = 0.4 * bruto + 0.6 * intencao
# ==============================

# CHAVE = termo de busca (sem acento para evitar encoding issues)
# VALOR = cesta de intencao (com acento para resultados corretos no Trends)
destinos_ancora = {
    "Monte Verde MG": [
        "pousada Monte Verde",
        "hotel Monte Verde",
        "o que fazer Monte Verde",
        "fim de semana Monte Verde",
    ],
    "Campos do Jordao": [
        "pousada Campos do Jordao",
        "hotel Campos do Jordao",
        "o que fazer Campos do Jordao",
        "inverno Campos do Jordao",
    ],
    "Santo Antonio do Pinhal": [
        "pousada Santo Antonio do Pinhal",
        "hotel Santo Antonio do Pinhal",
        "o que fazer Santo Antonio do Pinhal",
        "fim de semana Santo Antonio do Pinhal",
    ],
    "Visconde de Maua": [
        "pousada Visconde de Maua",
        "chale Visconde de Maua",
        "o que fazer Visconde de Maua",
        "fim de semana Visconde de Maua",
    ],
    "Serra Negra SP": [
        "pousada Serra Negra",
        "hotel Serra Negra",
        "o que fazer Serra Negra",
        "fim de semana Serra Negra",
    ],
    "Petropolis RJ": [
        "pousada Petropolis",
        "hotel Petropolis",
        "o que fazer Petropolis",
        "fim de semana Petropolis",
    ],
    "Nova Friburgo": [
        "pousada Nova Friburgo",
        "hotel Nova Friburgo",
        "o que fazer Nova Friburgo",
        "fim de semana Nova Friburgo",
    ],
    "Gramado RS": [
        "pousada Gramado",
        "hotel Gramado",
        "o que fazer Gramado",
        "fim de semana Gramado",
    ],
    "Canela RS": [
        "pousada Canela",
        "hotel Canela RS",
        "o que fazer Canela",
        "fim de semana Canela",
    ],
}

# IDs explícitos — garantia de consistência com o banco Supabase
DESTINO_ID_MAP = {
    "Monte Verde MG":          "monte_verde_mg",
    "Campos do Jordao":        "campos_do_jordao",
    "Santo Antonio do Pinhal": "santo_antonio_do_pinhal",
    "Visconde de Maua":        "visconde_de_maua",
    "Serra Negra SP":          "serra_negra_sp",
    "Petropolis RJ":           "petropolis_rj",
    "Nova Friburgo":           "nova_friburgo",
    "Gramado RS":              "gramado_rs",
    "Canela RS":               "canela_rs",
}

# ==============================
# DESTINOS CONCORRENTES (9)
# Bruto simples — usado para IPCR
# Tupla: (termo de busca, destino_id no banco)
# ==============================

destinos_concorrentes = [
    ("Penedo RJ",         "penedo_rj"),
    ("Teresopolis",       "teresopolis"),
    ("Tiradentes MG",     "tiradentes_mg"),
    ("Itaipava RJ",       "itaipava_rj"),
    ("Lavras Novas",      "lavras_novas"),
    ("Urubici",           "urubici"),
    ("Miguel Pereira RJ", "miguel_pereira_rj"),
    ("Sao Joaquim SC",    "sao_joaquim_sc"),
    ("Ouro Preto",        "ouro_preto"),
]

# ==============================
# FUNÇÕES AUXILIARES
# ==============================

def sleep_progressivo(tentativa):
    tempo = BASE_SLEEP * (BACKOFF_FACTOR ** tentativa) + random.uniform(0, 2)
    print(f"    aguardando {tempo:.0f}s...")
    time.sleep(tempo)

def coletar_interesse_bruto(termo_busca):
    """Coleta interesse bruto de um unico termo."""
    for tentativa in range(MAX_RETRIES):
        try:
            pytrends.build_payload([termo_busca], timeframe=TIMEFRAME, geo=GEO)
            dados = pytrends.interest_over_time()
            return int(dados[termo_busca].mean()) if not dados.empty else 0
        except TooManyRequestsError:
            sleep_progressivo(tentativa)
        except Exception as e:
            print(f"    Erro em {termo_busca}: {e}")
            sleep_progressivo(tentativa)
    return 0

def coletar_origens_bruto(termo_busca):
    """Coleta top 3 origens brutas apos build_payload."""
    for tentativa in range(MAX_RETRIES):
        try:
            regioes = pytrends.interest_by_region(
                resolution='REGION',
                inc_low_vol=True
            )
            if regioes.empty or termo_busca not in regioes.columns:
                break
            top3 = regioes.sort_values(by=termo_busca, ascending=False).head(3)
            origens = top3.index.tolist()
            valores = top3[termo_busca].tolist()
            while len(origens) < 3:
                origens.append("none")
                valores.append(0)
            return (
                origens[0].lower().replace(" ", "_"), int(valores[0]),
                origens[1].lower().replace(" ", "_"), int(valores[1]),
                origens[2].lower().replace(" ", "_"), int(valores[2])
            )
        except TooManyRequestsError:
            sleep_progressivo(tentativa)
        except Exception as e:
            print(f"    Erro origens {termo_busca}: {e}")
            sleep_progressivo(tentativa)
    return ("none", 0, "none", 0, "none", 0)

def coletar_cesta_intencao(destino_nome, termos_busca):
    """
    Coleta interesse bruto + cesta de intencao para ancora.
    Retorna interesse_final (0.4*bruto + 0.6*intencao) e origens qualificadas.
    """
    # 1. Bruto do destino principal
    interesse_bruto = coletar_interesse_bruto(destino_nome)
    time.sleep(random.uniform(3, 5))

    # 2. Origens brutas (fallback)
    pytrends.build_payload([destino_nome], timeframe=TIMEFRAME, geo=GEO)
    o1_bruto, p1_bruto, o2_bruto, p2_bruto, o3_bruto, p3_bruto = coletar_origens_bruto(destino_nome)
    time.sleep(random.uniform(3, 5))

    # 3. Cesta de intencao - media dos 4 termos
    interesse_intencao_total = 0
    origens_intencao = {}

    for termo in termos_busca:
        for tentativa in range(MAX_RETRIES):
            try:
                pytrends.build_payload([termo], timeframe=TIMEFRAME, geo=GEO)
                dados = pytrends.interest_over_time()
                interesse_termo = int(dados[termo].mean()) if not dados.empty else 0
                interesse_intencao_total += interesse_termo

                regioes = pytrends.interest_by_region(
                    resolution='REGION',
                    inc_low_vol=True
                )
                if not regioes.empty and termo in regioes.columns:
                    for estado in regioes.index:
                        val = int(regioes.loc[estado, termo])
                        if val > 0:
                            origens_intencao[estado] = origens_intencao.get(estado, 0) + val
                break
            except TooManyRequestsError:
                sleep_progressivo(tentativa)
            except Exception as e:
                print(f"    Erro cesta {termo}: {e}")
                sleep_progressivo(tentativa)

        time.sleep(random.uniform(4, 7))

    # 4. Interesse final = 0.4 * bruto + 0.6 * media intencao
    interesse_intencao_media = interesse_intencao_total // len(termos_busca) if termos_busca else 0
    interesse_final = int(0.4 * interesse_bruto + 0.6 * interesse_intencao_media)

    # 5. Top 3 origens qualificadas — normalizado para escala 0-100
    if origens_intencao:
        sorted_origens = sorted(origens_intencao.items(), key=lambda x: x[1], reverse=True)[:3]
        while len(sorted_origens) < 3:
            sorted_origens.append(("none", 0))
        # Normalizar: estado #1 = 100, demais proporcionais
        max_val = sorted_origens[0][1] if sorted_origens[0][1] > 0 else 1
        o1 = sorted_origens[0][0].lower().replace(" ", "_")
        p1 = 100
        o2 = sorted_origens[1][0].lower().replace(" ", "_")
        p2 = int(round(sorted_origens[1][1] / max_val * 100)) if sorted_origens[1][1] > 0 else 0
        o3 = sorted_origens[2][0].lower().replace(" ", "_")
        p3 = int(round(sorted_origens[2][1] / max_val * 100)) if sorted_origens[2][1] > 0 else 0
    else:
        o1, p1, o2, p2, o3, p3 = o1_bruto, p1_bruto, o2_bruto, p2_bruto, o3_bruto, p3_bruto

    return interesse_final, o1, p1, o2, p2, o3, p3

# ==============================
# COLETA ANCORA
# ==============================

def coletar_destinos_ancora():
    resultado = []
    for destino_nome, termos in destinos_ancora.items():
        destino_id = DESTINO_ID_MAP[destino_nome]
        print(f"  {destino_nome} -> id: {destino_id}")
        interesse, o1, p1, o2, p2, o3, p3 = coletar_cesta_intencao(destino_nome, termos)
        resultado.append([
            data_brasil(),
            destino_id,
            interesse,
            o1, p1,
            o2, p2,
            o3, p3
        ])
        print(f"    interesse={interesse} origem_1={o1}({p1})")
        time.sleep(random.uniform(5, 8))
    return resultado

# ==============================
# COLETA CONCORRENTES (bruto)
# ==============================

def coletar_destinos_concorrentes():
    resultado = []
    for destino_nome, destino_id in destinos_concorrentes:
        print(f"  {destino_nome} -> id: {destino_id}")
        interesse = coletar_interesse_bruto(destino_nome)
        time.sleep(random.uniform(3, 5))
        pytrends.build_payload([destino_nome], timeframe=TIMEFRAME, geo=GEO)
        o1, p1, o2, p2, o3, p3 = coletar_origens_bruto(destino_nome)
        resultado.append([
            data_brasil(),
            destino_id,
            interesse,
            o1, p1,
            o2, p2,
            o3, p3
        ])
        print(f"    interesse={interesse} origem_1={o1}({p1})")
        time.sleep(random.uniform(4, 6))
    return resultado

# ==============================
# INSERIR NO SUPABASE
# ==============================

def inserir_supabase(rows, tipo):
    if not SUPABASE_URL or not SUPABASE_KEY:
        print("Variaveis Supabase nao definidas - pulando insercao.")
        return

    headers = {
        "apikey": SUPABASE_KEY,
        "Authorization": f"Bearer {SUPABASE_KEY}",
        "Content-Type": "application/json",
        "Prefer": "return=minimal"
    }

    payload = []
    for row in rows:
        payload.append({
            "data_coleta":  row[0],
            "destino_id":   row[1],
            "interesse":    row[2],
            "origem_1":     row[3],
            "origem_1_pct": row[4],
            "origem_2":     row[5],
            "origem_2_pct": row[6],
            "origem_3":     row[7],
            "origem_3_pct": row[8],
            "tipo":         tipo,
        })

    r = requests.post(
        f"{SUPABASE_URL}/rest/v1/pulse_serras",
        headers=headers,
        json=payload,
        timeout=30
    )

    if r.status_code in (200, 201):
        print(f"  {len(payload)} registros ({tipo}) inseridos no Supabase.")
    else:
        print(f"  Erro Supabase {r.status_code}: {r.text}")
        sys.exit(1)

# ==============================
# EXECUCAO PRINCIPAL
# ==============================

print("PULSE SERRAS - Coleta com cesta de intencao")
print("=" * 50)
print(f"Ancora: {len(destinos_ancora)} destinos")
print(f"Concorrentes: {len(destinos_concorrentes)} destinos")

# ANCORA
print("\nColetando destinos ancora (cesta de intencao)...")
resultado_ancora = coletar_destinos_ancora()

if len(resultado_ancora) == 0:
    print("ERRO: Nenhum dado coletado para destinos ancora.")
    sys.exit(1)

with open('coleta-serras-ancora.csv', 'w', newline='', encoding='utf-8') as f:
    writer = csv.writer(f)
    writer.writerow(['data_coleta','destino_id','interesse',
                     'origem_1','origem_1_pct','origem_2','origem_2_pct','origem_3','origem_3_pct'])
    writer.writerows(resultado_ancora)

print(f"CSV ancora gerado ({len(resultado_ancora)} registros).")
inserir_supabase(resultado_ancora, "ancora")

# CONCORRENTES
print("\nColetando destinos concorrentes (bruto)...")
resultado_concorrentes = coletar_destinos_concorrentes()

if len(resultado_concorrentes) == 0:
    print("ERRO: Nenhum dado coletado para destinos concorrentes.")
    sys.exit(1)

with open('coleta-serras-concorrentes.csv', 'w', newline='', encoding='utf-8') as f:
    writer = csv.writer(f)
    writer.writerow(['data_coleta','destino_id','interesse',
                     'origem_1','origem_1_pct','origem_2','origem_2_pct','origem_3','origem_3_pct'])
    writer.writerows(resultado_concorrentes)

print(f"CSV concorrentes gerado ({len(resultado_concorrentes)} registros).")
inserir_supabase(resultado_concorrentes, "concorrente")

print("\nColeta Pulse Serras concluida com sucesso.")
