from pytrends.request import TrendReq
import pandas as pd
from datetime import datetime
import csv

# Inicializa Google Trends
pytrends = TrendReq(hl='pt-BR', tz=180)

# Destinos monitorados
destinos = [
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

hoje = datetime.today().strftime('%Y-%m-%d')

resultado = []

for destino in destinos:
    pytrends.build_payload([destino], timeframe='now 7-d', geo='BR')
    dados = pytrends.interest_over_time()
    
    if not dados.empty:
        interesse = int(dados[destino].mean())
    else:
        interesse = 0
    
    resultado.append([hoje, destino.lower().replace(" ", "_"), interesse])

# Gera CSV automaticamente
with open('coleta-trends-para.csv', 'w', newline='', encoding='utf-8') as f:
    writer = csv.writer(f)
    writer.writerow(['data_coleta','destino_id','interesse'])
    writer.writerows(resultado)

print("Coleta automática concluída com sucesso.")
