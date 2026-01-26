/* ===============================
   DEMAND PULSE AMAZÔNIA – RADAR.JS
   Base estável | sem Supabase
   =============================== */

document.addEventListener("DOMContentLoaded", function () {
  var anchor = document.getElementById("radar-anchor");
  if (!anchor) return;

  // Dados simulados
  var destinos = [
    { nome: "Belém", interesse: 78, variacao: 4.2 },
    { nome: "Alter do Chão", interesse: 92, variacao: 6.8 },
    { nome: "Santarém", interesse: 64, variacao: -1.5 },
    { nome: "Marabá", interesse: 51, variacao: 2.1 },
    { nome: "Parauapebas", interesse: 47, variacao: -0.8 }
  ];

  var html = '<div class="comparison-section">';

  for (var i = 0; i < destinos.length; i++) {
    var d = destinos[i];
    var dir = d.variacao >= 0 ? "up" : "down";
    var sinal = d.variacao >= 0 ? "+" : "";

    html += '<div class="card">';
    html += '  <div class="card-top">';
    html += '    <span class="dest-name">' + d.nome + '</span>';
    html += '    <span class="badge ' + dir + '">' + sinal + d.variacao + '%</span>';
    html += '  </div>';
    html += '  <div class="metric">';
    html += '    <span class="metric-label">Interesse</span>';
    html += '    <span class="metric-value">' + d.interesse + '</span>';
    html += '  </div>';
    html += '</div>';
  }

  html += '</div>';

  anchor.innerHTML = html;

  var boletim = document.getElementById("boletim-texto");
  if (boletim) {
    boletim.innerText =
      "BOLETIM ESTRATÉGICO — AMAZÔNIA\n\n" +
      "• Alter do Chão lidera interesse\n" +
      "• Belém mantém crescimento estável\n" +
      "• Santarém apresenta leve retração\n\n" +
      "(Base simulada — Supabase entra na próxima etapa)";
  }

  console.log("✅ Radar carregado sem uso de crase");
});
