import os
import csv
from supabase import create_client

print("🌴 PULSE AMAZÔNIA - IMPORTAÇÃO DEFINITIVA")

SUPABASE_URL = os.environ.get("SUPABASE_URL")
SUPABASE_KEY = os.environ.get("SUPABASE_KEY")

if not SUPABASE_URL or not SUPABASE_KEY:
    raise Exception("Variáveis de ambiente SUPABASE_URL ou SUPABASE_KEY não encontradas.")

supabase = create_client(SUPABASE_URL, SUPABASE_KEY)

print("🔗 Conexão Supabase estabelecida")

CSV_PATH = os.environ.get("CSV_PATH", "coleta-trends-para.csv")

if not os.path.exists(CSV_PATH):
    raise Exception(f"Arquivo CSV não encontrado: {CSV_PATH}")

print(f"📄 CSV: {CSV_PATH}")

with open(CSV_PATH, newline='', encoding='utf-8') as csvfile:
    reader = csv.DictReader(csvfile)

    required_columns = [
        "data_coleta",
        "destino_id",
        "interesse",
        "origem_1",
        "origem_1_pct",
        "origem_2",
        "origem_2_pct",
        "origem_3",
        "origem_3_pct"
    ]

    for col in required_columns:
        if col not in reader.fieldnames:
            raise Exception(f"Coluna obrigatória ausente no CSV: {col}")

    registros = []
    linhas_processadas = 0

    for row in reader:
        try:
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
            linhas_processadas += 1

        except Exception as e:
            print(f"⚠️ Linha ignorada por erro de conversão: {row}")
            print(f"Erro: {e}")

    print(f"📊 Registros válidos: {linhas_processadas}")

if registros:
    response = supabase.table("pulse_amazonia").upsert(
        registros,
        on_conflict="destino_id,data_coleta"
    ).execute()

    print("✅ Importação concluída com sucesso!")
    print(f"📈 Total enviado ao Supabase: {len(registros)} registros")
else:
    print("⚠️ Nenhum registro válido para importar.")
