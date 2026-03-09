const CACHE_NAME = 'pulse-amazonia-v1';
const urlsToCache = [
  '/radar-amazonia.html',
  '/manifest.json',
  '/img/logo-abr-allinone.png'
];

self.addEventListener('install', event => {
  event.waitUntil(
    caches.open(CACHE_NAME).then(cache => cache.addAll(urlsToCache))
  );
});

self.addEventListener('fetch', event => {
  event.respondWith(
    caches.match(event.request).then(response => {
      return response || fetch(event.request);
    })
  );
});
