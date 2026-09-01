const EXAMPLES = {
  "Cross join": `SELECT o.order_id, c.email
FROM analytics.orders o
CROSS JOIN analytics.customers c
WHERE o.order_date >= '2024-01-01';`,
  "Comma join": `SELECT o.order_id, c.email
FROM analytics.orders o, analytics.customers c
WHERE o.customer_id = c.customer_id
  AND o.order_date >= '2024-01-01';`,
  "Filtered join": `SELECT o.order_id, c.email, o.amount
FROM analytics.orders o
INNER JOIN analytics.customers c
  ON c.customer_id = o.customer_id
WHERE o.order_date >= DATEADD(day, -7, CURRENT_DATE())
  AND o.status = 'FULFILLED'
LIMIT 1000;`,
  "Non-sargable": `SELECT *
FROM fact.page_views
WHERE YEAR(event_date) = 2024
  AND (status = 'ok' OR status = 'late' OR status = 'retry');`,
  "Flatten explode": `SELECT u.user_id, f.value:sku::string AS sku
FROM raw.events u,
LATERAL FLATTEN(input => u.payload:items) f
WHERE u.event_day >= '2026-01-01';`,
  "Heavy sort": `SELECT *
FROM fact.page_views
WHERE event_date >= '2023-01-01'
ORDER BY event_ts DESC;`,
};

const sqlEl = document.getElementById("sql");
const examplesEl = document.getElementById("examples");
Object.entries(EXAMPLES).forEach(([name, sql]) => {
  const b = document.createElement("button");
  b.type = "button";
  b.textContent = name;
  b.addEventListener("click", () => {
    sqlEl.value = sql.trim();
  });
  examplesEl.appendChild(b);
});
sqlEl.value = EXAMPLES["Cross join"].trim();

const results = document.getElementById("results");
const statusEl = document.getElementById("status");
const runBtn = document.getElementById("run");

runBtn.addEventListener("click", analyze);
document.addEventListener("keydown", (e) => {
  if ((e.metaKey || e.ctrlKey) && e.key === "Enter") analyze();
});

async function analyze() {
  statusEl.textContent = "Analyzing…";
  runBtn.disabled = true;
  let explain = document.getElementById("explain").value.trim();
  let explainJson = null;
  if (explain) {
    try {
      explainJson = JSON.parse(explain);
    } catch {
      // Snowflake worksheet copies are often a JSON blob with a header; the API unwraps strings.
      explainJson = explain;
    }
  }
  const body = {
    sql: sqlEl.value,
    warehouse_size: document.getElementById("warehouse").value,
    explain_json: explainJson,
    account: val("account"),
    user: val("user"),
    password: val("password"),
    warehouse: val("sfWarehouse"),
    database: val("database"),
    schema: val("schema"),
  };
  try {
    const res = await fetch("/api/dry-run", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify(body),
    });
    const data = await res.json();
    if (!res.ok) throw new Error(data.detail || res.statusText);
    render(data);
    statusEl.textContent = data.connected
      ? "Used live Snowflake EXPLAIN."
      : data.source === "pasted_json"
        ? "Used pasted EXPLAIN JSON."
        : "Local synthetic plan (no Snowflake session).";
  } catch (err) {
    statusEl.textContent = err.message || String(err);
    results.hidden = true;
  } finally {
    runBtn.disabled = false;
  }
}

function val(id) {
  const v = document.getElementById(id).value.trim();
  return v || null;
}

function render(data) {
  results.hidden = false;
  const w = data.warehouse;
  document.getElementById("metrics").innerHTML = [
    metric("Health", `${data.score} · ${data.score_label}`),
    metric("Plan source", data.source),
    metric("Given WH", w.given_size),
    metric("Suggested WH", w.recommended_size),
    metric("Runtime on given", fmtDuration(w.estimated_seconds_on_given)),
    metric("Runtime on suggested", fmtDuration(w.estimated_seconds_on_recommended)),
    metric("Credits (given)", String(w.credit_hours_on_given)),
  ].join("");

  const ul = document.getElementById("findings");
  ul.innerHTML = "";
  if (!data.findings.length) {
    ul.innerHTML = `<li class="empty">No row-explosion or scan red flags from this plan.</li>`;
  } else {
    data.findings.forEach((f) => {
      const li = document.createElement("li");
      li.className = f.severity;
      li.innerHTML = `<h3><span class="badge">${f.severity}</span>${esc(f.title)}</h3>
        <p>${esc(f.detail)}</p>
        ${f.hint ? `<p>${esc(f.hint)}</p>` : ""}`;
      ul.appendChild(li);
    });
  }

  const notes = document.getElementById("notes");
  notes.innerHTML = (data.static_notes || []).map((n) => `<li>${esc(n)}</li>`).join("");
  document.getElementById("rationale").innerHTML = (w.rationale || [])
    .map((n) => `<li>${esc(n)}</li>`)
    .join("");
  document.getElementById("scale").textContent = w.scale_note || "";

  const wrap = document.getElementById("rewrites-wrap");
  const box = document.getElementById("rewrites");
  if (!data.rewrites || !data.rewrites.length) {
    wrap.hidden = true;
    box.innerHTML = "";
  } else {
    wrap.hidden = false;
    box.innerHTML = data.rewrites
      .map((rw, i) => {
        const kind = rw.safe ? "safe" : "optional";
        return `<article class="rewrite ${kind}">
          <div class="rewrite-head">
            <h3><span class="badge">${kind}</span>${esc(rw.title)}</h3>
            <button type="button" class="use-sql" data-idx="${i}">Load into editor</button>
          </div>
          <p>${esc(rw.reason)}</p>
          <pre>${esc(rw.sql)}</pre>
        </article>`;
      })
      .join("");
    box.querySelectorAll(".use-sql").forEach((btn) => {
      btn.addEventListener("click", () => {
        const rw = data.rewrites[Number(btn.getAttribute("data-idx"))];
        if (rw) sqlEl.value = rw.sql;
      });
    });
  }

  const warnIds = new Set(data.findings.flatMap((f) => f.operator_ids || []));
  document.getElementById("ops").innerHTML = (data.plan.nodes || [])
    .map((n) => {
      const bytes = n.bytes_assigned != null ? fmtBytes(n.bytes_assigned) : "—";
      const parts =
        n.partitions_assigned != null
          ? `${n.partitions_assigned}/${n.partitions_total ?? "?"}`
          : "—";
      return `<tr class="${warnIds.has(n.id) ? "warn" : ""}">
        <td>${n.id}</td>
        <td>${esc(n.operation)}</td>
        <td>${esc((n.objects || []).join(", "))}</td>
        <td>${esc((n.expressions || []).join(" | "))}</td>
        <td>${bytes}</td>
        <td>${parts}</td>
      </tr>`;
    })
    .join("");
}

function metric(label, value) {
  return `<div class="metric"><span>${esc(label)}</span><strong>${esc(value)}</strong></div>`;
}

function esc(s) {
  return String(s ?? "")
    .replaceAll("&", "&amp;")
    .replaceAll("<", "&lt;")
    .replaceAll(">", "&gt;");
}

function fmtDuration(s) {
  const n = Number(s);
  if (!Number.isFinite(n)) return String(s);
  if (n < 90) return `${n}s`;
  if (n < 90 * 60) return `${(n / 60).toFixed(1)} min`;
  return `${(n / 3600).toFixed(1)} h`;
}

function fmtBytes(n) {
  const u = ["B", "KiB", "MiB", "GiB", "TiB"];
  let v = n;
  let i = 0;
  while (v >= 1024 && i < u.length - 1) {
    v /= 1024;
    i += 1;
  }
  return i === 0 ? `${n} B` : `${v.toFixed(1)} ${u[i]}`;
}
