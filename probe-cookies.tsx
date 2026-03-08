import("./src/browser/chromeCookies.ts")
  .then((m) => m.loadChromeCookies({ targetUrl: "https://chatgpt.com" }))
  .then((c) => console.log("cookies", c.length))
  .catch((e) => console.error("err", e?.message || e));
