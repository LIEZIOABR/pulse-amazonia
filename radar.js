/* =========================================================
   RADAR.JS — DEMAND PULSE AMAZÔNIA
   Fonte de verdade • Supabase JS v2 • Sem erro de console
========================================================= */

(function () {
  // ---------- VALIDAÇÃO BÁSICA ----------
  if (!window.supabase) {
    console.error("Supabase JS não carregado (window.supabase)");
    return;
  }

  if (!window.SUPABASE_URL || !window.SUPABASE_ANON_KEY) {
    console.error("Credenciais Supabase ausentes");
    return;
  }

  // ---------- CLIENTE SUPABASE ----------
  const supabase = window.supabase.createClient(
    window.SUPABASE_URL,
    window.SUPABASE_ANON_KEY
  );

  console.log("Supabase client inicializado com sucesso");

  // ---------- ELEMENTO ÂNCORA ----------
  const anchor = document.getElementById("radar-anchor");
  if (!anchor) {
    console.error("Elemento #radar-anchor não encontrado");
    return;
  }

  // ---------- RENDERIZAÇÃO ----------
  function renderDestino(row) {
    const card = document.createElement("div");
    card.className = "radar-card";

    card.innerHTML = `
      <h3>${row.destino_id}</h3>
      <p><strong>Interesse:</strong> ${row.interesse}</p>
      <p><strong>Origem 1:</strong> ${row.origem_1} (${row.origem_1_pct}%)</p>
      <p><strong>Origem 2:</strong> ${row.origem_2} (${row.origem_2_pct}%)</p>
      <p class="radar-date">${row.data_coleta}</p>
    `;

    anchor.appendChild(card);
  }

  // ---------- CARGA DE DADOS ----------
  async function carregarRadar() {
    try {
      const { data, error } = await supabase
        .from("pulse_amazonia")
        .select("*")
        .order("interesse", { ascending: false });

      if (error) {
        console.error("Erro Supabase:", error);
        return;
      }

      if (!data || data.length === 0) {
        console.warn("Nenhum dado encontrado");
        return;
      }

      anchor.innerHTML = "";
      data.forEach(renderDestino);

      console.log("Radar carregado com sucesso");
    } catch (err) {
      console.error("Erro inesperado:", err);
    }
  }

  // ---------- START ----------
  document.addEventListener("DOMContentLoaded", carregarRadar);
})();
