/* ===============================
   DEMAND PULSE AMAZÔNIA – RADAR.JS
   Base estável | Supabase ativo
   =============================== */

document.addEventListener("DOMContentLoaded", function () {

  var anchor = document.getElementById("radar-anchor");
  if (!anchor) return;

  var destinos = [];

  async function carregarDestinos() {
    try {
      const { data, error } = await supabase
        .from('pulse_amazonia')
        .select('destino_id, interesse')
        .order('interesse', { ascending: false })
        .limit(5);

      if (error) {
        console.error("Erro Supabase:", error);
        return;
      }

      destinos = data.map(function (d) {
        return {
          nome: d.destino_id,
          interesse: d.interesse,
          variacao: 0
        };
      });

      renderRadar();

    } catch (e) {
      console.error("Erro geral:", e);
    }
  }

  function renderRadar() {
    var html = "<div class='comparison-section'>";

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
        "• Ranking dinâmico por interesse\n" +
        "• Dados reais via Supabase\n" +
        "• Atualização automática\n\n" +
        "(Base operacional)";
    }

    console.log("✅ Radar carregado com Supabase");
  }

  carregarDestinos();

});
