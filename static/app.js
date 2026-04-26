// alert("APP JS LOADED v99");



let historyStack = [];

function saveCurrentState() {
  const out = document.getElementById("out");
  const input = document.getElementById("q");

  if (out && out.innerHTML.trim()) {
    historyStack.push({
      html: out.innerHTML,
      query: input.value
    });
  }
}

function goBack() {
  if (!historyStack.length) return;

  const prev = historyStack.pop();
  document.getElementById("out").innerHTML = prev.html;
  document.getElementById("q").value = prev.query;
}

function runFromRef(ref) {
  saveCurrentState();

  const input = document.getElementById("q");
  input.value = ref;
  run();
}

// make available to onclick=""
window.goBack = goBack;
window.runFromRef = runFromRef;





function verseCard(v) {
  let html = `
    <div class="result-card">
      <div style="font-weight:600">${v.ref}</div>
      <div style="font-size:14px;opacity:.9">${v.text || ""}</div>
      ${v.corpus ? `<div class="muted">source: ${v.corpus}</div>` : ""}
  `;

  if (v.precepts && v.precepts.length) {
    html += `<div style="margin-top:10px"><b>Precepts</b></div>`;
    html += v.precepts.map(p => `
      <div style="margin-top:6px;padding:8px;border:1px solid #2f2f3f;border-radius:8px;background:#11111a;">
        <div style="font-weight:600">${p.ref}</div>
        <div style="font-size:13px">${p.text || ""}</div>
        <div class="muted">${p.corpus || ""} • ${p.source || ""}</div>
      </div>
    `).join("");
  }

  if (v.near_misses && v.near_misses.length) {
    html += `<div style="margin-top:10px"><b>Next Closest</b></div>`;
    html += v.near_misses.map(n => `
      <div style="font-size:13px;margin-top:4px">${typeof n === "string" ? n : (n.ref || JSON.stringify(n))}</div>
    `).join("");
  }


  if (v.strong && v.strong.length) {

    html += `<div style="margin-top:10px"><b>Strong's</b></div>`;

    html += v.strong.map(s => `
      <div style="margin-top:6px;padding:8px;border:1px solid #2f2f3f;border-radius:8px;background:#11111a;">
        <div style="font-weight:600">${s.code}</div>

        ${s.lemma ? `<div style="font-size:14px">${s.lemma}</div>` : ""}

        ${s.xlit ? `<div style="font-size:13px"><i>${s.xlit}</i></div>` : ""}

        ${s.pronounce ? `<div style="font-size:12px;opacity:.7">${s.pronounce}</div>` : ""}

        ${s.definition ? `<div style="font-size:13px;margin-top:4px">${s.definition}</div>` : ""}

      </div>
    `).join("");

  }

  html += `</div>`;
  return html;
}

function preceptCard(p) {
  return `
    <div class="result-card">
      <div style="font-weight:600">${p.ref}</div>
      <div style="font-size:14px">${p.text || ""}</div>
      <div class="muted">
        ${p.corpus || ""} • ${p.source || ""} • confidence ${Math.round((p.confidence || 0) * 100)}%
      </div>
    </div>
  `;
}


function clearSelect(id, placeholder) {
  const el = document.getElementById(id);
  if (!el) return;

  el.innerHTML = "";

  const opt = document.createElement("option");
  opt.value = "";
  opt.textContent = placeholder;
  el.appendChild(opt);
}

function fillSelect(id, values, placeholder) {
  clearSelect(id, placeholder);

  const el = document.getElementById(id);
  if (!el) return;

  values.forEach((value) => {
    const opt = document.createElement("option");
    opt.value = String(value);
    opt.textContent = String(value);
    el.appendChild(opt);
  });
}




function buildReferenceQuery() {
  const book = document.getElementById("book").value.trim();
  const chapter = document.getElementById("chapter").value.trim();
  const verse = document.getElementById("verse").value.trim();

  let q = "";

  if (book) {
    q = book;
    if (chapter) q += " " + chapter;
    if (verse) q += ":" + verse;
  }

  return q;
}


function getFinalQuery() {
  const qEl = document.getElementById("q");
  const mainQuery = qEl.value.trim();

  const corpus = document.getElementById("corpus").value.trim().toLowerCase();
  const book = document.getElementById("book").value.trim();
  const chapter = document.getElementById("chapter").value.trim();
  const verse = document.getElementById("verse").value.trim();

  const hasNav = book || chapter || verse;

  let q = "";

  if (hasNav) {
    // ✅ STRICT: nav builds ONLY reference queries
    q = buildReferenceQuery();
  } else {
    // ✅ STRICT: main box handles text search
    q = mainQuery;

    // append corpus if not already present
    if (corpus && !/--corpus\s*=/.test(q)) {
      q += ` --corpus=${corpus}`;
    }
  }

  return q.trim();
}

function getLoadingMessage() {
  const book = document.getElementById("book").value.trim();
  const chapter = document.getElementById("chapter").value.trim();
  const verse = document.getElementById("verse").value.trim();

  if (book && chapter && !verse) return "Loading chapter results...";
  if (book && chapter && verse.includes("-")) return "Loading verse range...";
  if (book && chapter && verse) return "Loading verse...";
  return "Searching...";
}

async function loadCorpora() {
  const res = await fetch("/meta/corpora");
  const data = await res.json();

  const el = document.getElementById("corpus");
  if (!el) return;

  el.innerHTML = "";

  const autoOpt = document.createElement("option");
  autoOpt.value = "";
  autoOpt.textContent = "Auto";
  el.appendChild(autoOpt);

  (data.corpora || []).forEach((item) => {
    const opt = document.createElement("option");
    opt.value = item.key;
    opt.textContent = item.label;
    el.appendChild(opt);
  });
}

async function loadBooks(corpus) {
  clearSelect("book", "Select book");
  clearSelect("chapter", "Select chapter");

  if (!corpus) return;

  const res = await fetch(`/meta/books?corpus=${encodeURIComponent(corpus)}`);
  const data = await res.json();

  const books = data.books || [];
  fillSelect("book", books, "Select book");

  if (books.length === 1) {
    const bookEl = document.getElementById("book");
    bookEl.value = books[0];
    await loadChapters(corpus, books[0]);
  }
}

async function loadChapters(corpus, book) {
  clearSelect("chapter", "Select chapter");

  if (!corpus || !book) return;

  const res = await fetch(
    `/meta/chapters?corpus=${encodeURIComponent(corpus)}&book=${encodeURIComponent(book)}`
  );
  const data = await res.json();

  fillSelect("chapter", data.chapters || [], "Select chapter");
}


function runFromRef(ref) {
  const input = document.getElementById("q");
  input.value = ref;
  run();
}





function goBack() {
  if (historyStack.length === 0) return;

  const prev = historyStack.pop();

  const out = document.getElementById("out");
  const input = document.getElementById("q");

  out.innerHTML = prev.html;
  input.value = prev.query;
}



async function run() {
  const out = document.getElementById("out");
  const searchBtn = document.getElementById("searchBtn");
  const q = getFinalQuery();
  

  if (!q) {
    out.innerHTML = '<div class="hint">Enter a query.</div>';
    return;
  }

  searchBtn.disabled = true;
  searchBtn.textContent = "Searching...";
  
  const loadingMsg = getLoadingMessage();
  out.innerHTML = `
    <div class="loading">
      <div class="spinner"></div>
      <div>${loadingMsg}</div>
    </div>
  `;

  try {
    const selectedCorpus = document.getElementById("corpus").value.trim();
    let url = '/query?q=' + encodeURIComponent(q);

    if (selectedCorpus) {
      url += '&corpus=' + encodeURIComponent(selectedCorpus);
    }

    const res = await fetch(url);
    const data = await res.json();
    



    if (data.error) {
      out.innerHTML = `<div style="color:#ff8080">${data.error}</div>`;
      return;
    }

    if (data.kind === "single_verse") {
      const r = data.result;
      let html = `
        <h3 style="margin-top:0">${r.ref}</h3>
        <div style="margin-bottom:10px">${r.text || ""}</div>
        <div class="muted" style="margin-bottom:12px">source: ${r.corpus || ""}</div>
      `;

      if (r.precepts && r.precepts.length) {
        html += `<h4>Precepts</h4>`;
        html += r.precepts.map(preceptCard).join("");
      }

      if (r.near_misses && r.near_misses.length) {
        html += `<h4>Next Closest</h4>`;
        html += r.near_misses.map(p => `<div style="font-size:13px">${p}</div>`).join("");
      }

      if (r.strong && r.strong.length) {
        html += `<h4>Strong's</h4>`;
        html += r.strong.map(s => `
          <div style="margin-top:6px;padding:8px;border:1px solid #2f2f3f;border-radius:8px;background:#11111a;">
            <div style="font-weight:600">${s.code}</div>
            ${s.lemma ? `<div style="font-size:14px">${s.lemma}</div>` : ""}
            ${s.xlit ? `<div style="font-size:13px"><i>${s.xlit}</i></div>` : ""}
            ${s.pronounce ? `<div style="font-size:12px;opacity:.7">${s.pronounce}</div>` : ""}
            ${s.definition ? `<div style="font-size:13px;margin-top:4px">${s.definition}</div>` : ""}
          </div>
        `).join("");
      }

      out.innerHTML = html;
      return;
    }

    if (
      data.kind === "chapter_range" ||
      data.kind === "verse_range" ||
      data.kind === "single_chapter"
    ) {
      const label =
        data.kind === "verse_range"
          ? `${data.book} ${data.range.start.chapter}:${data.range.start.verse}-${data.range.end.chapter}:${data.range.end.verse}`
          : data.kind === "single_chapter"
            ? `${data.book} ${data.chapter}`
            : `${data.book} ${data.chapters.start}-${data.chapters.end}`;

      let html = `
        <h3>${label}</h3>
        <div class="hint">corpus: ${data.corpus} • verses: ${data.count}</div>
        <div style="margin-top:12px">
      `;

      html += data.results.map(v => verseCard(v)).join("");
      html += `</div>`;
      out.innerHTML = html;
      return;
    }


    if (data.kind === "phrase_match") {
      const r = data.result;

      let html = `
        <h3 style="margin-top:0">${r.ref}</h3>
        <div style="margin-bottom:10px">${r.text || ""}</div>
        <div class="muted" style="margin-bottom:12px">source: ${r.corpus || ""}</div>
      `;

      if (r.note) {
        html += `<div class="hint" style="margin-bottom:12px;">${r.note}</div>`;
      }

      if (r.precepts && r.precepts.length) {
        html += `<h4>Precepts</h4>`;
        html += r.precepts.map(preceptCard).join("");
      }

      out.innerHTML = html;
      return;
    }




    if(data.kind === "text_search"){

      let html = `
        <h3>Search Results</h3>
        <div class="hint">${data.count} matches</div>
      `;

      html += data.matches.map(m => `
        <div style="margin-top:12px;padding:10px;border:1px solid #333;border-radius:10px;background:#0f0f18;">
          <div style="font-weight:600">${m.ref}</div>
          <div style="font-size:14px">${m.text}</div>
          <div style="font-size:12px;opacity:.6">score ${m.score.toFixed(2)}</div>
        </div>
      `).join("");

      if (data.next_refs && data.next_refs.length) {
        html += `
          <h4 style="margin-top:16px;">More results</h4>
        `;

        html += data.next_refs.map(r => `
          <div class="clickable-ref" onclick="runFromRef('${r.ref}')">
            ${r.ref}
          </div>
        `).join("");
      }

      out.innerHTML = html;
      return;
    }


    const q = getFinalQuery();

    if (out.innerHTML.trim()) {
      historyStack.push({
        html: out.innerHTML,
        query: q
      });
    }



    out.innerHTML = '<div class="hint">No renderer for result type.</div>';
  } catch (e) {
    out.innerHTML = `<div style="color:#ff8080">${e.message}</div>`;
  } finally {
    searchBtn.disabled = false;
    searchBtn.textContent = "Search";
  }
}

document.getElementById("searchBtn").addEventListener("click", run);
document.getElementById("q").addEventListener("keydown", (e) => {
  if (e.key === "Enter") run();
});

document.getElementById("corpus").addEventListener("change", (e) => {
  loadBooks(e.target.value);
});

document.getElementById("book").addEventListener("change", () => {
  loadChapters(
    document.getElementById("corpus").value,
    document.getElementById("book").value
  );
});

/*
document.getElementById("corpus").addEventListener("change", buildQueryFromNav);
document.getElementById("book").addEventListener("input", buildQueryFromNav);
document.getElementById("chapter").addEventListener("input", buildQueryFromNav);
document.getElementById("verse").addEventListener("input", buildQueryFromNav);
*/

loadCorpora();

if ("serviceWorker" in navigator) {
  navigator.serviceWorker
    .register("/static/service-worker.js")
    .then(() => console.log("Service Worker Registered"))
    .catch(err => console.error("Service Worker Failed:", err));
}