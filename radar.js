/* ===============================
   DEMAND PULSE AMAZÔNIA – RADAR.JS
   Base estável | sem Supabase
   =============================== */

document.addEventListener("DOMContentLoaded", () => {
  const anchor = document.getElementById("radar-anchor");
  if (!anchor) return;

  // Dados simulados (temporários)
  const destinos = [
    { nome: "Belém", interesse: 78, variacao: 4.2 },
    { nome: "Alter do Chão", interesse: 92, variacao: 6.8 },
    { nome: "Santarém", interesse: 64, variacao: -1.5 },
    { nome: "Marabá", interesse: 51, variacao: 2.1 },
    { nome: "Parauapebas", interesse: 47, variacao: -0.8 }
  ];

  // Monta cards
  let html = <div class="comparison-section">;

  destinos.forEach(d => {
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
  });

  html += </div>;

  anchor.innerHTML = html;

  // Atualiza boletim se existir
  const boletim = document.getElementById("boletim-texto");
  if (boletim) {
    boletim.innerText =
`BOLETIM ESTRATÉGICO — AMAZÔNIA

* Alter do Chão lidera interesse
* Belém mantém crescimento estável
* Santarém apresenta leve retração

(Base simulada — Supabase entra na próxima etapa)`;
  }

  console.log("✅ Radar carregado com dados simulados");
});
