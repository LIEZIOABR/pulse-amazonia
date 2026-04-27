from pytrends.request import TrendReq
from pytrends.exceptions import TooManyRequestsError
import pandas as pd
from datetime import datetime, timedelta, timezone
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
# DESTINOS ÂNCORA (12)
# ==============================

destinos_ancora = [
    "Monte Verde MG",
    "Campos do Jordão",
    "Gramado RS",
    "Canela RS",
    "Gonçalves MG",
    "São Bento do Sapucaí",
    "Santo Antônio do Pinhal",
    "Serra Negra SP",
    "Petrópolis RJ",
    "Visconde de Mauá",
    "Passa Quatro MG",
    "Nova Friburgo",
]

# ==============================
# DESTINOS CONCORRENTES (12)
# ==============================

destinos_concorrentes = [
    "Penedo RJ",
    "Teresópolis",
    "Tiradentes MG",
    "Itaipava RJ",
    "Lavras Novas",
    "Urubici",
    "Miguel Pereira RJ",
    "São Joaquim SC",
    "Ouro Preto",
    "Diamantina MG",
    "Triunfo PE",
    "Guaramiranga",
]

# ==============================
# FUNÇÕES AUXILIARES
# ==============================

def sleep_progressivo(tentativa):
    tempo = BASE_SLEEP * (BACKOFF_FACTOR ** tentativa) + random.uniform(0, 2)
    time.sleep(tempo)

def coletar_interesse(destino):
    for tentativa in range(MAX_RETRIES):
        try:
            pytrends.build_payload([destino], timeframe=TIMEFRAME, geo=GEO)
            dados = pytrends.interest_over_time()
            return int(dados[destino].mean()) if not dados.empty else 0
        except TooManyRequestsError:
            sleep_progressivo(tentativa)
        except Exception:
            sleep_progressivo(tentativa)
    return 0

def coletar_origens(destino):
    for tentativa in range(MAX_RETRIES):
        try:
            regioes = pytrends.interest_by_region(
                resolution='REGION',
                inc_low_vol=True
            )

            if regioes.empty or destino not in regioes.columns:
                break

            top3 = regioes.sort_values(by=destino, ascending=False).head(3)
            origens = top3.index.tolist()
            valores = top3[destino].tolist()

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
        except Exception:
            sleep_progressivo(tentativa)

    return ("none", 0, "none", 0, "none", 0)

def coletar_destinos(lista_destinos):
    resultado = []

    for destino in lista_destinos:
        interesse = coletar_interesse(destino)
        time.sleep(random.uniform(3, 5))

        o1, p1, o2, p2, o3, p3 = coletar_origens(destino)

        resultado.append([
            data_brasil(),
            destino.lower().replace(" ", "_"),
            interesse,
            o1, p1,
            o2, p2,
            o3, p3
        ])

        print(f"  ✓ {destino} → interesse={interesse}")
        time.sleep(random.uniform(4, 6))

    return resultado

# ==============================
# INSERIR NO SUPABASE
# ==============================

def inserir_supabase(rows, tipo):
    if not SUPABASE_URL or not SUPABASE_KEY:
        print("⚠️  SUPABASE_URL ou SUPABASE_SERVICE_ROLE_KEY não definidos — pulando inserção.")
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
        print(f"  ✅ {len(payload)} registros ({tipo}) inseridos no Supabase.")
    else:
        print(f"  ❌ Erro Supabase {r.status_code}: {r.text}")
        sys.exit(1)

# ==============================
# COLETA ÂNCORA
# ==============================

print("🏔️  Coletando destinos âncora...")
resultado_ancora = coletar_destinos(destinos_ancora)

if len(resultado_ancora) == 0:
    print("❌ ERRO: Nenhum dado coletado para destinos âncora.")
    sys.exit(1)

with open('coleta-serras-ancora.csv', 'w', newline='', encoding='utf-8') as f:
    writer = csv.writer(f)
    writer.writerow(['data_coleta','destino_id','interesse',
                     'origem_1','origem_1_pct','origem_2','origem_2_pct','origem_3','origem_3_pct'])
    writer.writerows(resultado_ancora)

print(f"✅ CSV âncora gerado ({len(resultado_ancora)} registros).")
inserir_supabase(resultado_ancora, "ancora")

# ==============================
# COLETA CONCORRENTES
# ==============================

print("\n🏁 Coletando destinos concorrentes...")
resultado_concorrentes = coletar_destinos(destinos_concorrentes)

if len(resultado_concorrentes) == 0:
    print("❌ ERRO: Nenhum dado coletado para destinos concorrentes.")
    sys.exit(1)

with open('coleta-serras-concorrentes.csv', 'w', newline='', encoding='utf-8') as f:
    writer = csv.writer(f)
    writer.writerow(['data_coleta','destino_id','interesse',
                     'origem_1','origem_1_pct','origem_2','origem_2_pct','origem_3','origem_3_pct'])
    writer.writerows(resultado_concorrentes)

print(f"✅ CSV concorrentes gerado ({len(resultado_concorrentes)} registros).")
inserir_supabase(resultado_concorrentes, "concorrente")

print("\n🏁 Coleta Pulse Serras concluída com sucesso.")
