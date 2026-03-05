/* ======================================================
   Pulse Amazônia – radar.js (VERSÃO CORRIGIDA E ESTÁVEL)
   Fonte real de data: tabela pulse_amazonia
   Supabase JS v2
   ====================================================== */

/* === 1) CREDENCIAIS ================================== */
const SUPABASE_URL = "https://ljeoxnxezjmfbqngoqxz.supabase.co";
const SUPABASE_ANON_KEY = "sb_publishable_3omH8_Hx47S3AlmGOoycpw_Etp5P7Nh";

/* === 2) GUARDAS ====================================== */
if (!SUPABASE_URL || !SUPABASE_ANON_KEY) {
  console.warn("Supabase não configurado. Radar em modo visual.");
}

/* === 3) CLIENTE SUPABASE ============================== */
let supabase = null;

if (SUPABASE_URL && SUPABASE_ANON_KEY && window.supabase) {
  supabase = window.supabase.createClient(
    SUPABASE_URL,
    SUPABASE_ANON_KEY
  );
  console.info("Supabase client inicializado");
}

/* === 4) HELPERS ======================================= */
const $ = (id) => document.getElementById(id);

const setText = (id, value, fallback = "—") => {
  const el = $(id);
  if (!el) return;

  el.textContent =
    value === null || value === undefined || value === ""
      ? fallback
      : value;
};

const joinTop3 = (arr) => {
  if (!Array.isArray(arr) || arr.length === 0) return "—";
  return arr.slice(0, 3).join(", ");
};

/* === 5) RENDER ======================================== */
function renderCards(payload) {

  const origemDominante =
    payload?.origem_dominante ||
    payload?.dominant_origin ||
    payload?.top_origin ||
    "—";

  const top3 =
    payload?.top_3_origens ||
    payload?.topOrigins ||
    payload?.top_3 ||
    [];

  const perfilPublico =
    payload?.perfil_publico ||
    payload?.public_profile ||
    "—";

  const statusDemanda =
    payload?.status_demanda ||
    payload?.demand_status ||
    "—";

  setText("card-origem", origemDominante);
  setText("card-top3", joinTop3(top3));
  setText("card-perfil", perfilPublico);
  setText("card-status", statusDemanda);
}

/* === 6) DATA REAL DO SISTEMA ========================== */
async function getLatestDate() {

  const { data, error } = await supabase
    .from("pulse_amazonia")
    .select("data_coleta")
    .order("data_coleta", { ascending: false })
    .limit(1)
    .maybeSingle();

  if (error) {
    console.error("Erro ao buscar data real:", error.message);
    return null;
  }

  if (!data) {
    console.warn("Nenhuma data encontrada.");
    return null;
  }

  return data.data_coleta;
}

/* === 7) SNAPSHOT ====================================== */
async function loadLatestSnapshot() {

  if (!supabase) {
    console.info("Radar sem Supabase.");
    return;
  }

  const { data, error } = await supabase
    .from("demand_pulse_snapshots")
    .select("*")
    .order("created_at", { ascending: false })
    .limit(1)
    .maybeSingle();

  if (error) {
    console.error("Erro ao buscar snapshot:", error.message);
    return;
  }

  if (!data) {
    console.warn("Nenhum snapshot encontrado.");
    return;
  }

  const payload = data.payload ? data.payload : data;

  renderCards(payload);
}

/* === 8) BOOT ========================================== */
document.addEventListener("DOMContentLoaded", async () => {

  setText("card-origem", "—");
  setText("card-top3", "—");
  setText("card-perfil", "—");
  setText("card-status", "—");

  await loadLatestSnapshot();

  const ultimaData = await getLatestDate();

  if (ultimaData) {
    const el = document.querySelector("#boletim-data");
    if (el) el.textContent = ultimaData;
  }

});

/* === 9) BOTÃO ATUALIZAR DADOS ========================= */
function atualizarDados() {
  window.location.replace(window.location.pathname + '?v=' + Date.now());
}
