const CACHE_NAME = 'airtransfer-v2';

self.addEventListener('install', (e) => {
  self.skipWaiting();
});

self.addEventListener('activate', (e) => {
  return self.clients.claim();
});

self.addEventListener('fetch', (e) => {
  // network-first ودائماً بدون كاش HTTP حتى نضمن جلب أحدث نسخة أثناء التحديثات
  e.respondWith(fetch(e.request, { cache: 'no-store' }).catch(() => caches.match(e.request)));
});