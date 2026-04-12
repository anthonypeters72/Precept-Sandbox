const CACHE = "precept-cache-v1";

const ASSETS = [
  "/",
  "/static/index.html",
  "/static/app.js",
  "/static/styles.css"
];

self.addEventListener("install", event => {
  event.waitUntil(
    caches.open(CACHE).then(cache => cache.addAll(ASSETS))
  );
});

self.addEventListener("fetch", event => {
  event.respondWith(
    caches.match(event.request).then(res => {
      return res || fetch(event.request);
    })
  );
});