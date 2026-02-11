from pytrends.request import TrendReq
import pandas as pd
from datetime import datetime
import csv
import time

pytrends = TrendReq(hl='pt-BR', tz=180)

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

hoje = datetime.today().strftime('%Y-%m-%d')

# ==============================
# FUNÇÃO PADRÃO DE COLETA
# ==============================

def coletar_destinos(lista_destinos):
    resultado = []

    for destino in lista_destinos:
        print(f"Coletando {destino}...")

        pytrends.build_payload([destino], timeframe='now 7-d', geo='BR')
        dados = pytrends.interest_over_time()

        if not dados.empty:
            interesse = int(dados[destino].mean())
        else:
            interesse = 0

        time.sleep(2)

        # Interesse por sub-região
        regioes = pytrends.interest_by_region(resolution='REGION', inc_low_vol=True)

        if not regioes.empty:
            top3 = regioes.sort_values(by=destino, ascending=False).head(3)

            origens = top3.index.tolist()
            valores = top3[destino].tolist()

            while len(origens) < 3:
                origens.append("none")
                valores.append(0)

            origem_1 = origens[0].lower().replace(" ", "_")
            origem_2 = origens[1].lower().replace(" ", "_")
            origem_3 = origens[2].lower().replace(" ", "_")

            origem_1_pct = int(valores[0])
            origem_2_pct = int(valores[1])
            origem_3_pct = int(valores[2])

        else:
            origem_1 = "none"
            origem_2 = "none"
            origem_3 = "none"
            origem_1_pct = 0
            origem_2_pct = 0
            origem_3_pct = 0

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

        time.sleep(2)

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

print("CSV Pará gerado com sucesso.")

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
            linha[0],  # data
            linha[1],  # destino_slug
            linha[2]   # interesse
        ])

print("CSV Concorrentes gerado com sucesso.")
print("Coleta automática concluída.")
