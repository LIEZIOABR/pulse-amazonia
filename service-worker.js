const CACHE_NAME = 'abr-pwa-v1';
const FILES_TO_CACHE = [
  '/',
  '/index.html',
  '/css/style.css',
  '/js/main.js',
  '/img/logo-abr-allinone.png',
  '/img/icon-192.png',
  '/img/icon-512.png'
];

self.addEventListener('install', event => {
  console.log('[Service Worker] Instalando...');
  event.waitUntil(
    caches.open(CACHE_NAME)
      .then(cache => {
        console.log('[Service Worker] Cacheando arquivos da app');
        return cache.addAll(FILES_TO_CACHE);
      })
      .catch(error => {
        console.error('[Service Worker] Erro ao cachear arquivos:', error);
      })
  );
  self.skipWaiting();
});

self.addEventListener('activate', event => {
  console.log('[Service Worker] Ativando...');
  event.waitUntil(
    caches.keys().then(keys => {
      return Promise.all(
        keys.filter(key => key !== CACHE_NAME)
            .map(key => {
              console.log('[Service Worker] Removendo cache antigo:', key);
              return caches.delete(key);
            })
      );
    })
  );
  self.clients.claim();
});

self.addEventListener('fetch', event => {
  // Estratégia: Network First para HTML, Cache First para assets
  const url = new URL(event.request.url);
  
  if (event.request.method !== 'GET') {
    return;
  }

  // Network First para HTML (sempre busca versão mais recente)
  if (event.request.headers.get('accept').includes('text/html')) {
    event.respondWith(
      fetch(event.request)
        .then(response => {
          const responseClone = response.clone();
          caches.open(CACHE_NAME).then(cache => {
            cache.put(event.request, responseClone);
          });
          return response;
        })
        .catch(() => {
          return caches.match(event.request);
        })
    );
    return;
  }

  // Cache First para CSS, JS, imagens (performance)
  event.respondWith(
    caches.match(event.request)
      .then(response => {
        if (response) {
          return response;
        }
        return fetch(event.request)
          .then(response => {
            // Cache apenas respostas válidas
            if (!response || response.status !== 200 || response.type === 'error') {
              return response;
            }
            const responseClone = response.clone();
            caches.open(CACHE_NAME).then(cache => {
              cache.put(event.request, responseClone);
            });
            return response;
          });
      })
  );
});
