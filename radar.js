/* ======================================================
   Pulse Amazônia – radar.js (VERSÃO LIMPA E ESTÁVEL)
   Única fonte de verdade do Supabase
   Supabase JS v2
   ====================================================== */

/* === 1) CREDENCIAIS (COLE AS SUAS) ==================== */
const SUPABASE_URL = "https://ljeoxnxezjmfbqngoqxz.supabase.co";
const SUPABASE_ANON_KEY = "sb_publishable_3omH8_Hx47S3AlmGOoycpw_Etp5P7Nh";

/* === 2) GUARDAS BÁSICAS =============================== */
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
  console.info("Supabase client inicializado com sucesso");
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
  // Fallbacks defensivos: nada quebra se o campo não existir
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

/* === 6) FETCH PRINCIPAL =============================== */
async function loadLatestSnapshot() {
  if (!supabase) {
    console.info("Radar sem Supabase. Mantendo placeholders.");
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

  // Alguns projetos salvam um objeto payload dentro da linha
  const payload = data.payload ? data.payload : data;

  renderCards(payload);
  console.info("Radar carregado com sucesso");
}

/* === 7) BOOT ========================================== */
document.addEventListener("DOMContentLoaded", () => {
  // Estado inicial (visual limpo)
  setText("card-origem", "—");
  setText("card-top3", "—");
  setText("card-perfil", "—");
  setText("card-status", "—");

  // Carrega dados reais (se houver)
  loadLatestSnapshot();
});
