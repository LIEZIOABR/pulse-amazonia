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
