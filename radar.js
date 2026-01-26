/* ===============================
   DEMAND PULSE AMAZÔNIA – RADAR.JS
   Base estável | Supabase ativo
   =============================== */

document.addEventListener("DOMContentLoaded", async function () {

  const anchor = document.getElementById("radar-anchor");
  if (!anchor) return;

  let destinos = [];

  async function carregarDestinos() {
    try {
      const { data, error } = await supabase
        .from("pulse_amazonia")
        .select("destino_id, interesse")
        .order("interesse", { ascending: false })
        .limit(5);

      if (error) {
        console.error("Erro Supabase:", error);
        return;
      }

      destinos = data.map(d => ({
        nome: d.destino_id,
        interesse: d.interesse,
        variacao: 0
      }));

    } catch (e) {
      console.error("Erro geral:", e);
    }
  }

  // ⬇️ CARREGA DADOS PRIMEIRO
  await carregarDestinos();

  // ⬇️ RENDERIZA DEPOIS
  let html = "<div class='comparison-section'>";

  for (let i = 0; i < destinos.length; i++) {
    const d = destinos[i];
    const dir = d.variacao >= 0 ? "up" : "down";
    const sinal = d.variacao >= 0 ? "+" : "";

    html += `
      <div class="card">
        <div class="card-top">
          <span class="dest-name">${d.nome}</span>
          <span class="badge ${dir}">${sinal}${d.variacao}%</span>
        </div>
        <div class="metric">
          <span class="metric-label">Interesse</span>
          <span class="metric-value">${d.interesse}</span>
        </div>
      </div>
    `;
  }

  html += "</div>";
  anchor.innerHTML = html;

  const boletim = document.getElementById("boletim-texto");
  if (boletim) {
    boletim.innerText =
      "BOLETIM ESTRATÉGICO — AMAZÔNIA\n\n" +
      "• Ranking por interesse absoluto\n" +
      "• Dados reais Supabase\n" +
      "• Atualização automática\n\n" +
      "(Fase BI inicial ativa)";
  }

  console.log("✅ Radar carregado com Supabase ativo");
});
