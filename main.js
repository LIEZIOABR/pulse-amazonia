/* ===================================
   ABR Marketing Estratégico & Consultoria Hoteleira
   JavaScript Principal
   =================================== */

// --- Matrix Rain Effect ---
const canvas = document.getElementById('matrix');
const ctx = canvas.getContext('2d');

function resize() {
  canvas.width = window.innerWidth;
  canvas.height = window.innerHeight;
}
window.addEventListener('resize', resize);
resize();

const letters = '01';
const fontSize = 14;
let columns = Math.floor(canvas.width / fontSize);
let drops = Array(columns).fill(1);

function draw() {
  ctx.fillStyle = 'rgba(0,0,0,0.06)';
  ctx.fillRect(0, 0, canvas.width, canvas.height);
  ctx.fillStyle = '#00c6ff';
  ctx.font = fontSize + 'px monospace';

  for (let i = 0; i < drops.length; i++) {
    const text = letters[Math.floor(Math.random() * letters.length)];
    ctx.fillText(text, i * fontSize, drops[i] * fontSize);
    if (drops[i] * fontSize > canvas.height && Math.random() > 0.975) {
      drops[i] = 0;
    }
    drops[i]++;
  }
}
setInterval(draw, 45);

// Recalcular colunas ao redimensionar
window.addEventListener('resize', function() {
  columns = Math.floor(canvas.width / fontSize);
  drops = Array(columns).fill(1);
});

// --- Modal Functions ---
let __abrScrollY = 0;

function openModal(id) {
  const el = document.getElementById(id);
  if (!el) return;
  __abrScrollY = window.scrollY || 0;
  el.classList.add('active');
  el.setAttribute('aria-hidden', 'false');
  document.body.style.overflow = 'hidden';
  document.body.classList.add('abr-modal-open');
}

function closeModal(id) {
  const el = document.getElementById(id);
  if (!el) return;
  el.classList.remove('active');
  el.setAttribute('aria-hidden', 'true');
  document.body.style.overflow = '';
  document.body.classList.remove('abr-modal-open');
  window.scrollTo(0, __abrScrollY);
}

// Fecha clicando fora
document.addEventListener('click', function(e) {
  const overlay = e.target.closest('.abr-modal-overlay');
  if (overlay && e.target === overlay) {
    closeModal(overlay.id);
  }
});

// Fecha com ESC
document.addEventListener('keydown', function(e) {
  if (e.key === 'Escape') {
    const open = document.querySelector('.abr-modal-overlay.active');
    if (open) closeModal(open.id);
  }
});
