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
# DESTINOS ÂNCORA (10)
# Cesta de intenção — 4 termos por destino
# Fórmula: índice_final = 0.4 * bruto + 0.6 * intenção
# ==============================

destinos_ancora = {
    "Monte Verde MG": [
        "pousada Monte Verde",
        "hotel Monte Verde",
        "o que fazer Monte Verde",
        "fim de semana Monte Verde",
    ],
    "Campos do Jordão": [
        "pousada Campos do Jordão",
        "hotel Campos do Jordão",
        "o que fazer Campos do Jordão",
        "inverno Campos do Jordão",
    ],
    "São Bento do Sapucaí": [
        "pousada São Bento do Sapucaí",
        "hotel São Bento do Sapucaí",
        "o que fazer São Bento do Sapucaí",
        "fim de semana São Bento do Sapucaí",
    ],
    "Santo Antônio do Pinhal": [
        "pousada Santo Antônio do Pinhal",
        "hotel Santo Antônio do Pinhal",
        "o que fazer Santo Antônio do Pinhal",
        "fim de semana Santo Antônio do Pinhal",
    ],
    "Visconde de Mauá": [
        "pousada Visconde de Mauá",
        "chalé Visconde de Mauá",
        "o que fazer Visconde de Mauá",
        "fim de semana Visconde de Mauá",
    ],
    "Serra Negra SP": [
        "pousada Serra Negra",
        "hotel Serra Negra",
        "o que fazer Serra Negra",
        "fim de semana Serra Negra",
    ],
    "Petrópolis RJ": [
        "pousada Petrópolis",
        "hotel Petrópolis",
        "o que fazer Petrópolis",
        "fim de semana Petrópolis",
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

# ==============================
# DESTINOS CONCORRENTES (10)
# Bruto simples — usado para IPCR
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
    "Guaramiranga",
]

# ==============================
# FUNÇÕES AUXILIARES
# ==============================

def sleep_progressivo(tentativa):
    tempo = BASE_SLEEP * (BACKOFF_FACTOR ** tentativa) + random.uniform(0, 2)
    print(f"    ⏳ aguardando {tempo:.0f}s...")
    time.sleep(tempo)

def coletar_interesse_bruto(destino):
    """Coleta interesse bruto de um único termo."""
    for tentativa in range(MAX_RETRIES):
        try:
            pytrends.build_payload([destino], timeframe=TIMEFRAME, geo=GEO)
            dados = pytrends.interest_over_time()
            return int(dados[destino].mean()) if not dados.empty else 0
        except TooManyRequestsError:
            sleep_progressivo(tentativa)
        except Exception as e:
            print(f"    ⚠️  Erro em {destino}: {e}")
            sleep_progressivo(tentativa)
    return 0

def coletar_origens_bruto(destino):
    """Coleta top 3 origens brutas após build_payload."""
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
        except Exception as e:
            print(f"    ⚠️  Erro origens {destino}: {e}")
            sleep_progressivo(tentativa)
    return ("none", 0, "none", 0, "none", 0)

def coletar_cesta_intencao(destino, termos):
    """
    Coleta interesse bruto + cesta de intenção para âncora.
    Retorna interesse_final (0.4*bruto + 0.6*intenção) e origens qualificadas.
    """
    # 1. Bruto do destino principal
    interesse_bruto = coletar_interesse_bruto(destino)
    time.sleep(random.uniform(3, 5))

    # 2. Origens brutas (para IPCR interno)
    pytrends.build_payload([destino], timeframe=TIMEFRAME, geo=GEO)
    o1_bruto, p1_bruto, o2_bruto, p2_bruto, o3_bruto, p3_bruto = coletar_origens_bruto(destino)
    time.sleep(random.uniform(3, 5))

    # 3. Cesta de intenção — média dos 4 termos
    interesse_intencao_total = 0
    origens_intencao = {}

    for termo in termos:
        for tentativa in range(MAX_RETRIES):
            try:
                pytrends.build_payload([termo], timeframe=TIMEFRAME, geo=GEO)
                dados = pytrends.interest_over_time()
                interesse_termo = int(dados[termo].mean()) if not dados.empty else 0
                interesse_intencao_total += interesse_termo

                # Origens por termo de intenção
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
                print(f"    ⚠️  Erro cesta {termo}: {e}")
                sleep_progressivo(tentativa)

        time.sleep(random.uniform(4, 7))

    # 4. Interesse final = 0.4 * bruto + 0.6 * média intenção
    interesse_intencao_media = interesse_intencao_total // len(termos) if termos else 0
    interesse_final = int(0.4 * interesse_bruto + 0.6 * interesse_intencao_media)

    # 5. Top 3 origens qualificadas (soma dos termos de intenção)
    if origens_intencao:
        sorted_origens = sorted(origens_intencao.items(), key=lambda x: x[1], reverse=True)[:3]
        while len(sorted_origens) < 3:
            sorted_origens.append(("none", 0))
        o1 = sorted_origens[0][0].lower().replace(" ", "_")
        p1 = sorted_origens[0][1]
        o2 = sorted_origens[1][0].lower().replace(" ", "_")
        p2 = sorted_origens[1][1]
        o3 = sorted_origens[2][0].lower().replace(" ", "_")
        p3 = sorted_origens[2][1]
    else:
        # fallback para bruto se intenção não retornou origens
        o1, p1, o2, p2, o3, p3 = o1_bruto, p1_bruto, o2_bruto, p2_bruto, o3_bruto, p3_bruto

    return interesse_final, o1, p1, o2, p2, o3, p3

# ==============================
# COLETA ÂNCORA
# ==============================

def coletar_destinos_ancora():
    resultado = []
    for destino, termos in destinos_ancora.items():
        print(f"  🏔️  {destino}...")
        interesse, o1, p1, o2, p2, o3, p3 = coletar_cesta_intencao(destino, termos)
        resultado.append([
            data_brasil(),
            destino.lower().replace(" ", "_").replace("ã", "a").replace("ô", "o").replace("é", "e").replace("ç", "c"),
            interesse,
            o1, p1,
            o2, p2,
            o3, p3
        ])
        print(f"    ✓ interesse={interesse} · origem_1={o1}({p1})")
        time.sleep(random.uniform(5, 8))
    return resultado

# ==============================
# COLETA CONCORRENTES (bruto)
# ==============================

def coletar_destinos_concorrentes():
    resultado = []
    for destino in destinos_concorrentes:
        print(f"  🏁  {destino}...")
        interesse = coletar_interesse_bruto(destino)
        time.sleep(random.uniform(3, 5))
        pytrends.build_payload([destino], timeframe=TIMEFRAME, geo=GEO)
        o1, p1, o2, p2, o3, p3 = coletar_origens_bruto(destino)
        resultado.append([
            data_brasil(),
            destino.lower().replace(" ", "_").replace("ã", "a").replace("ô", "o").replace("é", "e").replace("ç", "c"),
            interesse,
            o1, p1,
            o2, p2,
            o3, p3
        ])
        print(f"    ✓ interesse={interesse} · origem_1={o1}({p1})")
        time.sleep(random.uniform(4, 6))
    return resultado

# ==============================
# INSERIR NO SUPABASE
# ==============================

def inserir_supabase(rows, tipo):
    if not SUPABASE_URL or not SUPABASE_KEY:
        print("⚠️  Variáveis Supabase não definidas — pulando inserção.")
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
# EXECUÇÃO PRINCIPAL
# ==============================

print("🏔️  PULSE SERRAS — Coleta com cesta de intenção")
print("=" * 50)

# ÂNCORA
print("\n📍 Coletando destinos âncora (cesta de intenção)...")
resultado_ancora = coletar_destinos_ancora()

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

# CONCORRENTES
print("\n🏁 Coletando destinos concorrentes (bruto)...")
resultado_concorrentes = coletar_destinos_concorrentes()

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
