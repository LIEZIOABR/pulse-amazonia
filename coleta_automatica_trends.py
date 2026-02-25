from pytrends.request import TrendReq
from pytrends.exceptions import TooManyRequestsError
import pandas as pd
from datetime import datetime, timedelta
import csv
import time
import random

# ==============================
# CONFIGURAÇÕES GERAIS
# ==============================

MAX_RETRIES = 5
BASE_SLEEP = 4
BACKOFF_FACTOR = 2
TIMEFRAME = 'now 7-d'
GEO = 'BR'

pytrends = TrendReq(hl='pt-BR', tz=180)

# 🔒 DATA ESTÁVEL (UTC → Brasil)
hoje = (datetime.utcnow() - timedelta(hours=3)).strftime('%Y-%m-%d')

# ==============================
# DESTINOS PARÁ
# ==============================

destinos_para = [
    "Belem","Santarem","Maraba","Alter do Chao","Ilha do Marajo",
    "Salinopolis","Soure","Salvaterra","Mosqueiro","Monte Alegre",
    "Algodoal","Obidos","Parauapebas","Castanhal","Cameta"
]

# ==============================
# CONCORRENTES NACIONAIS
# ==============================

concorrentes_nacionais = [
    "Manaus","Sao Luis","Lencois Maranhenses","Jalapao",
    "Bonito","Presidente Figueiredo","Parintins","Atins"
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
            regioes = pytrends.interest_by_region(resolution='REGION', inc_low_vol=True)
            if regioes.empty:
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
        o1,p1,o2,p2,o3,p3 = coletar_origens(destino)
        resultado.append([
            hoje,
            destino.lower().replace(" ", "_"),
            interesse,
            o1,p1,o2,p2,o3,p3
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
        'data_coleta','destino_id','interesse',
        'origem_1','origem_1_pct',
        'origem_2','origem_2_pct',
        'origem_3','origem_3_pct'
    ])
    writer.writerows(resultado_para)

# ==============================
# COLETA CONCORRENTES
# ==============================

resultado_concorrentes = coletar_destinos(concorrentes_nacionais)

with open('coleta-concorrentes-nacionais.csv', 'w', newline='', encoding='utf-8') as f:
    writer = csv.writer(f)
    writer.writerow(['data_coleta','destino_id','interesse'])
    for linha in resultado_concorrentes:
        writer.writerow([linha[0], linha[1], linha[2]])

print("🏁 Coleta concluída com data estável.")
