from pytrends.request import TrendReq
import pandas as pd
from datetime import datetime
import csv
import time
import random
from pytrends.exceptions import TooManyRequestsError

# ==============================
# CONFIGURAÇÃO
# ==============================

pytrends = TrendReq(hl='pt-BR', tz=180)
hoje = datetime.today().strftime('%Y-%m-%d')

MAX_TENTATIVAS = 5
PAUSA_MIN = 4
PAUSA_MAX = 9

# ==============================
# DESTINOS PARÁ (15)
# ==============================

destinos_para = [
    "Belem", "Santarem", "Maraba", "Alter do Chao", "Ilha do Marajo",
    "Salinopolis", "Soure", "Salvaterra", "Mosqueiro", "Monte Alegre",
    "Algodoal", "Obidos", "Parauapebas", "Castanhal", "Cameta"
]

# ==============================
# CONCORRENTES NACIONAIS (8)
# ==============================

concorrentes_nacionais = [
    "Manaus", "Sao Luis", "Lencois Maranhenses", "Jalapao",
    "Bonito", "Presidente Figueiredo", "Parintins", "Atins"
]

# ==============================
# FUNÇÃO DE COLETA SEGURA
# ==============================

def coletar_destinos(lista_destinos):
    resultado = []

    for destino in lista_destinos:
        destino_slug = destino.lower().replace(" ", "_")
        print(f"Coletando {destino}...")

        tentativa = 1
        sucesso = False

        while tentativa <= MAX_TENTATIVAS and not sucesso:
            try:
                pytrends.build_payload([destino], timeframe='now 7-d', geo='BR')
                dados = pytrends.interest_over_time()

                if not dados.empty:
                    interesse = int(dados[destino].mean())
                else:
                    interesse = 0

                time.sleep(random.uniform(PAUSA_MIN, PAUSA_MAX))

                regioes = pytrends.interest_by_region(
                    resolution='REGION',
                    inc_low_vol=True
                )

                if not regioes.empty:
                    top3 = regioes.sort_values(by=destino, ascending=False).head(3)
                    origens = top3.index.tolist()
                    valores = top3[destino].tolist()
                else:
                    origens, valores = [], []

                while len(origens) < 3:
                    origens.append("none")
                    valores.append(0)

                resultado.append([
                    hoje,
                    destino_slug,
                    interesse,
                    origens[0].lower().replace(" ", "_"),
                    int(valores[0]),
                    origens[1].lower().replace(" ", "_"),
                    int(valores[1]),
                    origens[2].lower().replace(" ", "_"),
                    int(valores[2])
                ])

                sucesso = True
                time.sleep(random.uniform(PAUSA_MIN, PAUSA_MAX))

            except TooManyRequestsError:
                espera = tentativa * 20
                print(f"⚠️ 429 em {destino}. Tentativa {tentativa}/{MAX_TENTATIVAS}. Aguardando {espera}s.")
                time.sleep(espera)
                tentativa += 1

            except Exception as e:
                print(f"❌ Erro em {destino}: {e}")
                break

        if not sucesso:
            print(f"⚠️ Falha definitiva em {destino}. Registrando zeros.")
            resultado.append([
                hoje,
                destino_slug,
                0, "none", 0, "none", 0, "none", 0
            ])

    return resultado

# ==============================
# COLETA PARÁ
# ==============================

resultado_para = coletar_destinos(destinos_para)

with open('coleta-trends-para.csv', 'w', newline='', encoding='utf-8') as f:
    writer = csv.writer(f)
    writer.writerow([
        'data_coleta', 'destino_id', 'interesse',
        'origem_1', 'origem_1_pct',
        'origem_2', 'origem_2_pct',
        'origem_3', 'origem_3_pct'
    ])
    writer.writerows(resultado_para)

print("CSV Pará gerado com sucesso.")

# ==============================
# COLETA CONCORRENTES
# ==============================

resultado_concorrentes = coletar_destinos(concorrentes_nacionais)

with open('coleta-concorrentes-nacionais.csv', 'w', newline='', encoding='utf-8') as f:
    writer = csv.writer(f)
    writer.writerow(['data_coleta', 'destino_id', 'interesse'])
    for linha in resultado_concorrentes:
        writer.writerow([linha[0], linha[1], linha[2]])

print("CSV Concorrentes gerado com sucesso.")
print("Coleta automática concluída.")
