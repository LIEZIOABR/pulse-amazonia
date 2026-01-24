// radar.js — teste de injeção no anchor
document.addEventListener('DOMContentLoaded', () => {
  const anchor = document.getElementById('radar-anchor');
  if (!anchor) return;

  anchor.innerHTML = `
    <div style="
      margin:24px auto;
      padding:18px;
      border:1px dashed rgba(0,198,255,.5);
      border-radius:16px;
      text-align:center;
      color:#00c6ff;
      font-weight:600;
    ">
      Radar ativo ✔️ (conteúdo injetado com sucesso)
    </div>
  `;
});
