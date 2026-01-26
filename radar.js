/* ===============================
   DEMAND PULSE AMAZÔNIA – RADAR.JS
   Base estável | Supabase ativo
   =============================== */

document.addEventListener("DOMContentLoaded", function () {

  var anchor = document.getElementById("radar-anchor");
  if (!anchor) return;

  // proteção: Supabase precisa existir no HTML
  if (!window.supabaseClient) {
    console.error("Supabase client não encontrado (window.supabaseClient)");
    anchor.innerHTML = "<p style='opacity:.6'>Erro: Supabase não inicializado</p>";
    return;
  }

  carregarDestinos();

  async function carregarDestinos() {
    try {
      const { data, error } = await window.supabaseClient
        .from("pulse_amazonia")
        .select("destino_id, interesse")
        .order("interesse", { ascending: false })
        .limit(5);

      if (error) {
        console.error("Erro Supabase:", error);
        anchor.innerHTML = "<p style='opacity:.6'>Erro ao carregar dados</p>";
        return;
      }

      renderRadar(data || []);
      console.log("✅ Radar carregado com Supabase ativo");

    } catch (e) {
      console.error("Erro geral:", e);
      anchor.innerHTML = "<p style='opacity:.6'>Falha inesperada</p>";
    }
  }

  function renderRadar(dados) {
    if (!dados.length) {
      anchor.innerHTML = "<p style='opacity:.6'>Sem dados disponíveis</p>";
      return;
    }

    var html = "<div class='comparison-section'>";

    for (var i = 0; i < dados.length; i++) {
      var d = dados[i];

      html += "<div class='card'>";
      html += "  <div class='card-top'>";
      html += "    <span class='dest-name'>" + d.destino_id + "</span>";
      html += "  </div>";
      html += "  <div class='metric'>";
      html += "    <span class='metric-label'>Interesse</span>";
      html += "    <span class='metric-value'>" + d.interesse + "</span>";
      html += "  </div>";
      html += "</div>";
    }

    html += "</div>";
    anchor.innerHTML = html;
  }

});
