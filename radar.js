/* ======================================================
   DEMAND PULSE AMAZÔNIA — radar.js (BASE VISUAL ESTÁVEL)
   Objetivo: provar renderização JS no anchor
   Sem Supabase | Sem dados reais | Sem risco
====================================================== */

document.addEventListener('DOMContentLoaded', () => {
  const anchor = document.getElementById('radar-anchor');

  if (!anchor) {
    console.error('❌ radar-anchor não encontrado no HTML');
    return;
  }

  anchor.innerHTML = `
    <section class="panel" style="margin-top:20px">
      <div style="text-align:center">
        <h2 style="
          color:#00c6ff;
          font-weight:800;
          letter-spacing:1px;
          margin-bottom:12px
        ">
          RADAR ATIVO
        </h2>

        <p style="
          color:#9ca3af;
          font-size:.9rem;
          max-width:520px;
          margin:0 auto
        ">
          O JavaScript está carregando corretamente.<br>
          A próxima etapa será conectar dados reais.
        </p>

        <div style="
          margin-top:18px;
          display:inline-block;
          padding:10px 18px;
          border-radius:999px;
          border:1px solid rgba(0,198,255,.35);
          color:#00c6ff;
          font-family:'Fira Code', monospace;
          font-size:.75rem;
          background:rgba(0,0,0,.35)
        ">
          STATUS: OK
        </div>
      </div>
    </section>
  `;
});
