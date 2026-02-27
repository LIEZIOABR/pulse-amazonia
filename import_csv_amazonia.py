import os
import csv
from datetime import datetime
from supabase import create_client

print("🌴 PULSE AMAZÔNIA - IMPORTAÇÃO DEFINITIVA (SAFE MODE + FAIL FAST)")

SUPABASE_URL = os.environ.get("SUPABASE_URL")
SUPABASE_KEY = os.environ.get("SUPABASE_KEY")

if not SUPABASE_URL or not SUPABASE_KEY:
    raise Exception("❌ Variáveis SUPABASE_URL ou SUPABASE_KEY não encontradas.")

supabase = create_client(SUPABASE_URL, SUPABASE_KEY)
print("🔗 Conexão Supabase estabelecida")

CSV_PATH = os.environ.get("CSV_PATH", "coleta-trends-para.csv")

if not os.path.exists(CSV_PATH):
    raise Exception(f"❌ CSV não encontrado: {CSV_PATH}")

print(f"📄 CSV detectado: {CSV_PATH}")

# --------------------------------------------------
# 1️⃣ Buscar última data gravada no banco
# --------------------------------------------------
ultimo = (
    supabase
    .table("pulse_amazonia")
    .select("data_coleta")
    .order("data_coleta", desc=True)
    .limit(1)
    .execute()
)

ultima_data = None
if ultimo.data:
    ultima_data = datetime.strptime(
        ultimo.data[0]["data_coleta"], "%Y-%m-%d"
    ).date()

print(f"📅 Última data no banco: {ultima_data if ultima_data else 'nenhuma'}")

# --------------------------------------------------
# 2️⃣ Ler CSV e filtrar apenas registros novos
# --------------------------------------------------
registros = []
linhas_lidas = 0
linhas_ignoradas = 0

with open(CSV_PATH, newline="", encoding="utf-8") as csvfile:
    reader = csv.DictReader(csvfile)

    for row in reader:
        data_csv = datetime.strptime(row["data_coleta"], "%Y-%m-%d").date()

        if ultima_data and data_csv <= ultima_data:
            linhas_ignoradas += 1
            continue

        registro = {
            "data_coleta": row["data_coleta"],
            "destino_id": row["destino_id"],
            "interesse": int(row["interesse"]),
            "origem_1": row["origem_1"],
            "origem_1_pct": int(row["origem_1_pct"]),
            "origem_2": row["origem_2"],
            "origem_2_pct": int(row["origem_2_pct"]),
            "origem_3": row["origem_3"],
            "origem_3_pct": int(row["origem_3_pct"]),
        }

        registros.append(registro)
        linhas_lidas += 1

print(f"📊 Linhas novas detectadas: {linhas_lidas}")
print(f"⏭️ Linhas antigas ignoradas: {linhas_ignoradas}")

# --------------------------------------------------
# 3️⃣ REGRA DE OURO — SE NÃO INSERIU, É ERRO
# --------------------------------------------------
if not registros:
    raise Exception(
        "🚨 ERRO CRÍTICO: Nenhum registro novo para inserir no Supabase. "
        "Workflow interrompido para evitar falso positivo."
    )

# --------------------------------------------------
# 4️⃣ Inserção segura (UPSERT)
# --------------------------------------------------
supabase.table("pulse_amazonia").upsert(
    registros,
    on_conflict="destino_id,data_coleta"
).execute()

print("✅ Importação concluída com sucesso")
print(f"📈 Registros inseridos: {len(registros)}")
