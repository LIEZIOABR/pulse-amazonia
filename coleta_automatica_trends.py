from pytrends.request import TrendReq
from pytrends.exceptions import TooManyRequestsError
import pandas as pd
from datetime import datetime
import csv
import time
import random

# ==============================
# CONFIGURAÇÕES GERAIS
# ==============================

MAX_RETRIES = 5
BASE_SLEEP = 4      # segundos base
BACKOFF_FACTOR = 2  # exponencial
TIMEFRAME = 'now 7-d'
GEO = 'BR'

pytrends = TrendReq(hl='pt-BR', tz=180)

hoje = datetime.today().strftime('%Y-%m-%d')

# ==============================
# DESTINOS PARÁ (15)
# ==============================

destinos_para = [
    "Belem",
    "Santarem",
    "Maraba",
    "Alter do Chao",
    "Ilha do Marajo",
    "Salinopolis",
    "Soure",
    "Salvaterra",
    "Mosqueiro",
    "Monte Alegre",
    "Algodoal",
    "Obidos",
    "Parauapebas",
    "Castanhal",
    "Cameta"
]

# ==============================
# CONCORRENTES NACIONAIS (8)
# ==============================

concorrentes_nacionais = [
    "Manaus",
    "Sao Luis",
    "Lencois Maranhenses",
    "Jalapao",
    "Bonito",
    "Presidente Figueiredo",
    "Parintins",
    "Atins"
]

# ==============================
# FUNÇÕES AUXILIARES
# ==============================

def sleep_progressivo(tentativa):
    tempo = BASE_SLEEP * (BACKOFF_FACTOR ** tentativa) + random.uniform(0, 2)
    print(f"⏳ Aguardando {int(tempo)}s (backoff)")
    time.sleep(tempo)

def coletar_interesse(destino):
    for tentativa in range(MAX_RETRIES):
        try:
            pytrends.build_payload([destino], timeframe=TIMEFRAME, geo=GEO)
            dados = pytrends.interest_over_time()
            if not dados.empty:
                return int(dados[destino].mean())
            return 0
        except TooManyRequestsError:
            print(f"⚠️ Rate limit ao coletar interesse ({destino})")
            sleep_progressivo(tentativa)
        except Exception as e:
            print(f"❌ Erro inesperado interesse ({destino}): {e}")
            sleep_progressivo(tentativa)
    print(f"🚨 Falha definitiva interesse ({destino})")
    return 0

def coletar_origens(destino):
    for tentativa in range(MAX_RETRIES):
        try:
            regioes = pytrends.interest_by_region(
                resolution='REGION',
                inc_low_vol=True
            )

            if regioes.empty:
                return ("none", 0, "none", 0, "none", 0)

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
            print(f"⚠️ Rate limit ao coletar origens ({destino})")
            sleep_progressivo(tentativa)
        except Exception as e:
            print(f"❌ Erro inesperado origens ({destino}): {e}")
            sleep_progressivo(tentativa)

    print(f"🚨 Falha definitiva origens ({destino})")
    return ("none", 0, "none", 0, "none", 0)

# ==============================
# COLETA PADRÃO
# ==============================

def coletar_destinos(lista_destinos):
    resultado = []

    for destino in lista_destinos:
        print(f"📡 Coletando {destino}...")

        interesse = coletar_interesse(destino)

        time.sleep(random.uniform(3, 5))

        origem_1, origem_1_pct, origem_2, origem_2_pct, origem_3, origem_3_pct = coletar_origens(destino)

        destino_slug = destino.lower().replace(" ", "_")

        resultado.append([
            hoje,
            destino_slug,
            interesse,
            origem_1,
            origem_1_pct,
            origem_2,
            origem_2_pct,
            origem_3,
            origem_3_pct
        ])

        time.sleep(random.uniform(4, 6))

    return resultado

# ==============================
# COLETA PARÁ
# ==============================

resultado_para = coletar_destinos(destinos_para)

with open('coleta-trends-para.csv', 'w', newline='', encoding='utf-8') as f:
    writer = csv.writer(f)
    writer.writerow([
        'data_coleta',
        'destino_id',
        'interesse',
        'origem_1',
        'origem_1_pct',
        'origem_2',
        'origem_2_pct',
        'origem_3',
        'origem_3_pct'
    ])
    writer.writerows(resultado_para)

print("✅ CSV Pará gerado com sucesso.")

# ==============================
# COLETA CONCORRENTES
# ==============================

resultado_concorrentes = coletar_destinos(concorrentes_nacionais)

with open('coleta-concorrentes-nacionais.csv', 'w', newline='', encoding='utf-8') as f:
    writer = csv.writer(f)
    writer.writerow([
        'data_coleta',
        'destino_id',
        'interesse'
    ])

    for linha in resultado_concorrentes:
        writer.writerow([
            linha[0],
            linha[1],
            linha[2]
        ])

print("✅ CSV Concorrentes gerado com sucesso.")
print("🏁 Coleta automática concluída com controle de falhas.")
