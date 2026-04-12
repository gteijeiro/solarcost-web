const CACHE_NAME = "energy-costs-shell-v3";
const APP_SHELL = [
  "/static/app.css",
  "/static/app.js",
  "/static/icon.svg",
  "/manifest.webmanifest"
];

function isDocumentRequest(request) {
  return request.mode === "navigate" || request.destination === "document";
}

function isCacheableResponse(response) {
  return Boolean(response) && response.status === 200 && response.type === "basic" && !response.redirected;
}

self.addEventListener("install", function (event) {
  event.waitUntil(
    caches.open(CACHE_NAME).then(function (cache) {
      return cache.addAll(APP_SHELL);
    })
  );
  self.skipWaiting();
});

self.addEventListener("activate", function (event) {
  event.waitUntil(
    caches.keys().then(function (keys) {
      return Promise.all(
        keys.map(function (key) {
          if (key !== CACHE_NAME) {
            return caches.delete(key);
          }
          return null;
        })
      );
    })
  );
  self.clients.claim();
});

self.addEventListener("fetch", function (event) {
  if (event.request.method !== "GET") {
    return;
  }

  // Let the browser handle HTML navigations because auth flows use redirects.
  if (isDocumentRequest(event.request)) {
    event.respondWith(fetch(event.request));
    return;
  }

  event.respondWith(
    caches.match(event.request).then(function (cachedResponse) {
      if (cachedResponse) {
        return cachedResponse;
      }

      return fetch(event.request)
        .then(function (networkResponse) {
          if (!isCacheableResponse(networkResponse)) {
            return networkResponse;
          }
          const responseToCache = networkResponse.clone();
          caches.open(CACHE_NAME).then(function (cache) {
            cache.put(event.request, responseToCache);
          });
          return networkResponse;
        });
    })
  );
});
