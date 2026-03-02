import { useState, useRef, useEffect, useCallback } from "react";

const rawBase = (import.meta.env.VITE_API_URL) || '/api'
// Ensure base URL always points to the API root (ends with /api)
const API = rawBase.endsWith('/api') ? rawBase : rawBase.replace(/\/$/, '') + '/api'
// ── NF badge colors ─────────────────────────────────────────────────
const NF_META = {
  "1NF":  { color: "#ff6b6b", label: "1NF",  desc: "Atomic values" },
  "2NF":  { color: "#ffa94d", label: "2NF",  desc: "No partial deps" },
  "3NF":  { color: "#ffd43b", label: "3NF",  desc: "No transitive deps" },
  "BCNF": { color: "#69db7c", label: "BCNF", desc: "Superkey determinants" },
  "4NF":  { color: "#4dabf7", label: "4NF",  desc: "No MVDs" },
  "5NF":  { color: "#cc5de8", label: "5NF",  desc: "No join deps" },
};

const NF_ORDER = ["1NF","2NF","3NF","BCNF","4NF","5NF"];

function getNfColor(nf) { return NF_META[nf]?.color ?? "#888"; }

// ── Reusable badge ──────────────────────────────────────────────────
function NfBadge({ nf }) {
  const meta = NF_META[nf] ?? { color: "#888", label: nf, desc: "" };
  return (
    <span style={{
      background: meta.color + "22",
      border: `1px solid ${meta.color}55`,
      color: meta.color,
      borderRadius: 4,
      padding: "2px 8px",
      fontSize: 11,
      fontFamily: "'JetBrains Mono', monospace",
      fontWeight: 700,
      letterSpacing: 1,
    }}>{meta.label}</span>
  );
}

// ── Drop zone ───────────────────────────────────────────────────────
function DropZone({ onFile, disabled }) {
  const [drag, setDrag] = useState(false);
  const inputRef = useRef();

  const handleDrop = (e) => {
    e.preventDefault(); setDrag(false);
    const file = e.dataTransfer.files[0];
    if (file && (file.name.endsWith(".xlsx") || file.name.endsWith(".xls"))) onFile(file);
  };

  return (
    <div
      onClick={() => !disabled && inputRef.current.click()}
      onDragOver={(e) => { e.preventDefault(); if (!disabled) setDrag(true); }}
      onDragLeave={() => setDrag(false)}
      onDrop={handleDrop}
      style={{
        border: `2px dashed ${drag ? "#4dabf7" : "#2a3a4a"}`,
        borderRadius: 16,
        padding: "56px 32px",
        textAlign: "center",
        cursor: disabled ? "not-allowed" : "pointer",
        background: drag ? "rgba(77,171,247,0.06)" : "rgba(255,255,255,0.02)",
        transition: "all .2s",
        position: "relative",
        overflow: "hidden",
      }}
    >
      {/* Grid decoration */}
      <div style={{
        position:"absolute", inset:0, opacity:0.04,
        backgroundImage:"linear-gradient(#4dabf7 1px,transparent 1px),linear-gradient(90deg,#4dabf7 1px,transparent 1px)",
        backgroundSize:"32px 32px",
        pointerEvents:"none",
      }}/>

      <div style={{ fontSize: 48, marginBottom: 16 }}>📊</div>
      <div style={{ color: "#e0e8f0", fontSize: 18, fontWeight: 700, marginBottom: 8 }}>
        Drop your Excel file here
      </div>
      <div style={{ color: "#566a7f", fontSize: 13 }}>
        .xlsx or .xls · max 20 MB · all sheets analyzed
      </div>
      <div style={{
        marginTop: 24, display: "inline-block",
        background: "linear-gradient(135deg, #1971c2, #4dabf7)",
        color: "#fff", padding: "10px 28px", borderRadius: 8,
        fontSize: 13, fontWeight: 700, letterSpacing: 0.5,
      }}>
        Browse files
      </div>
      <input
        ref={inputRef} type="file"
        accept=".xlsx,.xls"
        style={{ display: "none" }}
        onChange={(e) => e.target.files[0] && onFile(e.target.files[0])}
      />
    </div>
  );
}

// ── Live terminal log ───────────────────────────────────────────────
function Terminal({ logs, status }) {
  const endRef = useRef();
  useEffect(() => { endRef.current?.scrollIntoView({ behavior: "smooth" }); }, [logs]);

  return (
    <div style={{
      background: "#070d12",
      border: "1px solid #162230",
      borderRadius: 12,
      fontFamily: "'JetBrains Mono', monospace",
      fontSize: 12,
      overflow: "hidden",
    }}>
      {/* Terminal header */}
      <div style={{
        background: "#0d1a24",
        padding: "10px 16px",
        display: "flex", alignItems: "center", gap: 8,
        borderBottom: "1px solid #162230",
      }}>
        {["#ff5f57","#febc2e","#28c840"].map((c, i) => (
          <div key={i} style={{ width:12, height:12, borderRadius:"50%", background:c }}/>
        ))}
        <span style={{ color: "#3d5a73", marginLeft: 8, fontSize: 11 }}>
          normalization.log
        </span>
        {status === "running" && (
          <span style={{ marginLeft:"auto", color:"#ffd43b", fontSize:11, display:"flex", alignItems:"center", gap:6 }}>
            <span style={{ display:"inline-block", width:7, height:7, borderRadius:"50%", background:"#ffd43b", animation:"pulse 1s infinite" }}/>
            RUNNING
          </span>
        )}
        {status === "done" && (
          <span style={{ marginLeft:"auto", color:"#69db7c", fontSize:11 }}>✓ COMPLETE</span>
        )}
        {status === "error" && (
          <span style={{ marginLeft:"auto", color:"#ff6b6b", fontSize:11 }}>✗ ERROR</span>
        )}
      </div>

      {/* Log output */}
      <div style={{ padding: "16px", maxHeight: 320, overflowY: "auto" }}>
        {logs.length === 0 && (
          <span style={{ color: "#2a3a4a" }}>Waiting for job…</span>
        )}
        {logs.map((line, i) => (
          <div key={i} style={{
            color: line.startsWith("  ✔") ? "#69db7c"
                 : line.startsWith("  ⚡") ? "#ffa94d"
                 : line.startsWith("❌") ? "#ff6b6b"
                 : line.startsWith("✅") ? "#69db7c"
                 : line.startsWith("🔎") ? "#4dabf7"
                 : "#8badb8",
            lineHeight: 1.8,
            whiteSpace: "pre-wrap",
            wordBreak: "break-word",
          }}>{line}</div>
        ))}
        {status === "running" && (
          <span style={{ color: "#4dabf7" }}>▌</span>
        )}
        <div ref={endRef}/>
      </div>
    </div>
  );
}

// ── NF pipeline visual ─────────────────────────────────────────────
function NfPipeline({ completedSteps = [] }) {
  return (
    <div style={{ display:"flex", alignItems:"center", gap:0, flexWrap:"wrap" }}>
      {NF_ORDER.map((nf, i) => {
        const done = completedSteps.includes(nf);
        const color = getNfColor(nf);
        return (
          <div key={nf} style={{ display:"flex", alignItems:"center" }}>
            <div style={{
              padding: "6px 14px",
              borderRadius: 6,
              background: done ? color + "22" : "#0d1a24",
              border: `1px solid ${done ? color : "#1a2a3a"}`,
              color: done ? color : "#2a3a4a",
              fontSize: 11,
              fontFamily: "'JetBrains Mono', monospace",
              fontWeight: 700,
              letterSpacing: 1,
              transition: "all .3s",
              boxShadow: done ? `0 0 12px ${color}33` : "none",
            }}>
              {done ? "✓ " : ""}{nf}
            </div>
            {i < NF_ORDER.length - 1 && (
              <div style={{
                width: 20, height: 1,
                background: done ? "#2a4a5a" : "#111d27",
              }}/>
            )}
          </div>
        );
      })}
    </div>
  );
}

// ── Table preview card ─────────────────────────────────────────────
function TableCard({ table, isSelected, onClick }) {
  return (
    <div
      onClick={onClick}
      style={{
        background: isSelected ? "rgba(77,171,247,0.08)" : "rgba(255,255,255,0.02)",
        border: `1px solid ${isSelected ? "#4dabf7" : "#1a2a3a"}`,
        borderRadius: 10,
        padding: "14px 16px",
        cursor: "pointer",
        transition: "all .2s",
      }}
    >
      <div style={{ display:"flex", justifyContent:"space-between", alignItems:"flex-start", marginBottom:8 }}>
        <div style={{ color:"#e0e8f0", fontWeight:700, fontSize:13, fontFamily:"'JetBrains Mono', monospace" }}>
          {table.name}
        </div>
        <NfBadge nf={table.nf} />
      </div>
      <div style={{ display:"flex", gap:12, fontSize:11, color:"#566a7f" }}>
        <span>📦 {table.rows} rows</span>
        <span>📋 {table.columns.length} cols</span>
      </div>
      {table.pk.length > 0 && (
        <div style={{ marginTop:6, fontSize:11, color:"#ffd43b" }}>
          🔑 {table.pk.join(", ")}
        </div>
      )}
      {table.fk.length > 0 && (
        <div style={{ marginTop:2, fontSize:11, color:"#69db7c" }}>
          🔗 FK → {table.fk.join(", ")}
        </div>
      )}
    </div>
  );
}

// ── Data preview table ─────────────────────────────────────────────
function DataPreview({ table }) {
  if (!table) return null;
  const { columns, pk, fk, preview } = table;

  return (
    <div style={{ overflowX:"auto" }}>
      <table style={{
        width:"100%", borderCollapse:"collapse",
        fontFamily:"'JetBrains Mono', monospace", fontSize:12,
      }}>
        <thead>
          <tr>
            {columns.map(col => (
              <th key={col} style={{
                padding: "8px 14px",
                background: pk.includes(col) ? "#1a2a0a"
                          : fk.includes(col) ? "#0a2a1a" : "#0d1a24",
                color: pk.includes(col) ? "#ffd43b"
                     : fk.includes(col) ? "#69db7c" : "#8badb8",
                fontWeight: 700,
                textAlign:"left",
                borderBottom: "1px solid #1a2a3a",
                whiteSpace:"nowrap",
              }}>
                {pk.includes(col) ? "🔑 " : fk.includes(col) ? "🔗 " : ""}
                {col}
              </th>
            ))}
          </tr>
        </thead>
        <tbody>
          {preview.map((row, ri) => (
            <tr key={ri} style={{
              background: ri % 2 === 0 ? "rgba(255,255,255,0.02)" : "transparent",
            }}>
              {columns.map(col => (
                <td key={col} style={{
                  padding: "7px 14px",
                  color: pk.includes(col) ? "#ffd43b"
                       : fk.includes(col) ? "#82c9a0" : "#c0cfd8",
                  borderBottom: "1px solid #0d1a24",
                  whiteSpace:"nowrap",
                  maxWidth: 200,
                  overflow:"hidden",
                  textOverflow:"ellipsis",
                }}>
                  {row[col] ?? <span style={{ color:"#2a3a4a" }}>null</span>}
                </td>
              ))}
            </tr>
          ))}
        </tbody>
      </table>
      {preview.length === 5 && (
        <div style={{ padding:"8px 14px", color:"#2a3a4a", fontSize:11 }}>
          — showing first 5 rows —
        </div>
      )}
    </div>
  );
}

// ── Schema diagram (simplified ER) ─────────────────────────────────
function SchemaMap({ tables }) {
  return (
    <div style={{
      display:"grid",
      gridTemplateColumns:"repeat(auto-fill, minmax(200px, 1fr))",
      gap:12,
    }}>
      {tables.map(t => (
        <div key={t.name} style={{
          background:"#070d12",
          border:`1px solid ${getNfColor(t.nf)}33`,
          borderTop:`2px solid ${getNfColor(t.nf)}`,
          borderRadius:8, padding:12,
        }}>
          <div style={{ color:"#e0e8f0", fontWeight:700, fontSize:12,
            fontFamily:"'JetBrains Mono', monospace", marginBottom:8 }}>
            {t.name}
          </div>
          {t.columns.map(col => (
            <div key={col} style={{
              display:"flex", alignItems:"center", gap:6,
              fontSize:11, color: t.pk.includes(col) ? "#ffd43b"
                              : t.fk.includes(col) ? "#69db7c" : "#566a7f",
              padding:"2px 0",
              borderBottom:"1px solid #0d1a24",
            }}>
              <span style={{ opacity:0.6, fontFamily:"monospace" }}>
                {t.pk.includes(col) ? "PK" : t.fk.includes(col) ? "FK" : "  "}
              </span>
              {col}
            </div>
          ))}
        </div>
      ))}
    </div>
  );
}

// ════════════════════════════════════════════════════════════════════
//  MAIN APP
// ════════════════════════════════════════════════════════════════════
export default function App() {
  const [file,        setFile]        = useState(null);
  const [jobId,       setJobId]       = useState(null);
  const [status,      setStatus]      = useState("idle"); // idle|uploading|running|done|error
  const [logs,        setLogs]        = useState([]);
  const [tables,      setTables]      = useState([]);
  const [nfLog,       setNfLog]       = useState({});
  const [selectedTab, setSelectedTab] = useState("tables");
  const [selectedTbl, setSelectedTbl] = useState(null);
  const [errorMsg,    setErrorMsg]    = useState("");
  const esRef = useRef(null);

  // ── Derive completed NF steps from logs ──────────────────────────
  const completedSteps = NF_ORDER.filter(nf =>
    logs.some(l => l.includes(`[${nf}]`) || l.includes(`NF${nf[0]}`))
  );

  // ── Upload & start job ─────────────────────────────────────────
  const handleUpload = useCallback(async (f) => {
    setFile(f);
    setStatus("uploading");
    setLogs([]);
    setTables([]);
    setErrorMsg("");
    setSelectedTbl(null);

    const fd = new FormData();
    fd.append("file", f);

    try {
      const res  = await fetch(`${API}/normalize`, { method:"POST", body:fd });
      const data = await res.json();
      if (!res.ok) throw new Error(data.error || "Upload failed");

      const id = data.job_id;
      setJobId(id);
      setStatus("running");
      startSSE(id);
    } catch (err) {
      setStatus("error");
      setErrorMsg(err.message);
    }
  }, []);

  // ── SSE log streaming ──────────────────────────────────────────
  const startSSE = useCallback((id) => {
    if (esRef.current) esRef.current.close();
    const es = new EventSource(`${API}/stream/${id}`);
    esRef.current = es;

    es.onmessage = (e) => {
      const payload = JSON.parse(e.data);
      if (payload.log !== undefined) {
        setLogs(prev => [...prev, payload.log]);
      }
      if (payload.done) {
        es.close();
        // Fetch full results
        fetchResults(id);
      }
    };

    es.onerror = () => {
      es.close();
      fetchResults(id);
    };
  }, []);

  // ── Poll final result ──────────────────────────────────────────
  const fetchResults = useCallback(async (id) => {
    try {
      const res  = await fetch(`${API}/jobs/${id}`);
      const data = await res.json();
      if (data.status === "done") {
        setStatus("done");
        setTables(data.tables || []);
        setNfLog(data.nf_log || {});
        if (data.tables?.length) setSelectedTbl(data.tables[0]);
      } else if (data.status === "error") {
        setStatus("error");
        setErrorMsg(data.error || "Unknown error");
        setLogs(data.logs || []);
      } else {
        setTimeout(() => fetchResults(id), 800);
      }
    } catch {
      setTimeout(() => fetchResults(id), 1200);
    }
  }, []);

  const handleReset = () => {
    if (esRef.current) esRef.current.close();
    setFile(null); setJobId(null); setStatus("idle");
    setLogs([]); setTables([]); setNfLog({});
    setSelectedTbl(null); setErrorMsg("");
  };

  // ── Style helpers ──────────────────────────────────────────────
  const tabStyle = (t) => ({
    padding:"8px 20px", cursor:"pointer", fontSize:12,
    fontFamily:"'JetBrains Mono', monospace", fontWeight:700,
    letterSpacing:0.5, borderBottom:`2px solid ${selectedTab===t ? "#4dabf7" : "transparent"}`,
    color: selectedTab===t ? "#4dabf7" : "#566a7f",
    transition:"all .2s",
    background:"none", border:"none", borderBottom:`2px solid ${selectedTab===t ? "#4dabf7" : "transparent"}`,
  });

  const isProcessing = status === "running" || status === "uploading";

  // ════════════════════════════════════════════════════════════════
  return (
    <div style={{
      minHeight:"100vh",
      background:"#060e16",
      color:"#e0e8f0",
      fontFamily:"'DM Sans', sans-serif",
    }}>
      <style>{`
        @import url('https://fonts.googleapis.com/css2?family=DM+Sans:wght@400;500;700&family=JetBrains+Mono:wght@400;700&display=swap');
        * { box-sizing: border-box; margin:0; padding:0; }
        ::-webkit-scrollbar { width:6px; height:6px; }
        ::-webkit-scrollbar-track { background:#070d12; }
        ::-webkit-scrollbar-thumb { background:#1a2a3a; border-radius:3px; }
        @keyframes pulse { 0%,100%{opacity:1} 50%{opacity:.3} }
        @keyframes spin { to{transform:rotate(360deg)} }
        @keyframes fadeIn { from{opacity:0;transform:translateY(12px)} to{opacity:1;transform:none} }
        .fade-in { animation: fadeIn .4s ease forwards; }
        .btn-primary {
          background:linear-gradient(135deg,#1971c2,#4dabf7);
          color:#fff; border:none; border-radius:8px;
          padding:10px 24px; font-weight:700; cursor:pointer;
          font-size:13px; font-family:inherit;
          transition:opacity .2s;
        }
        .btn-primary:hover { opacity:.85; }
        .btn-ghost {
          background:transparent; color:#566a7f; border:1px solid #1a2a3a;
          border-radius:8px; padding:10px 24px; font-weight:700; cursor:pointer;
          font-size:13px; font-family:inherit; transition:all .2s;
        }
        .btn-ghost:hover { border-color:#4dabf7; color:#4dabf7; }
      `}</style>

      {/* ── Top bar ── */}
      <div style={{
        borderBottom:"1px solid #0d1a24",
        padding:"0 40px",
        display:"flex", alignItems:"center", justifyContent:"space-between",
        height:60,
        background:"rgba(6,14,22,0.9)",
        backdropFilter:"blur(12px)",
        position:"sticky", top:0, zIndex:100,
      }}>
        <div style={{ display:"flex", alignItems:"center", gap:14 }}>
          <div style={{
            width:36, height:36, borderRadius:9,
            background:"linear-gradient(135deg,#1971c2,#4dabf7)",
            display:"flex", alignItems:"center", justifyContent:"center",
            fontSize:18,
          }}>⚡</div>
          <div>
            <div style={{ fontWeight:800, fontSize:15, letterSpacing:-0.3 }}>NormalizerDB</div>
            <div style={{ fontSize:11, color:"#3d5a73", fontFamily:"'JetBrains Mono', monospace" }}>
              NF1 → NF5 · Excel → SQL
            </div>
          </div>
        </div>

        <div style={{ display:"flex", alignItems:"center", gap:16 }}>
          {status === "done" && (
            <>
              <a href={`${API}/download/${jobId}/excel`} download>
                <button className="btn-ghost">⬇ Excel</button>
              </a>
              <a href={`${API}/download/${jobId}/sql`} download>
                <button className="btn-primary">⬇ SQL</button>
              </a>
            </>
          )}
          {status !== "idle" && (
            <button className="btn-ghost" onClick={handleReset}>↩ New file</button>
          )}
        </div>
      </div>

      {/* ── Main content ── */}
      <div style={{ maxWidth:1200, margin:"0 auto", padding:"40px 24px" }}>

        {/* ════ IDLE STATE ════ */}
        {status === "idle" && (
          <div className="fade-in">
            {/* Hero */}
            <div style={{ textAlign:"center", marginBottom:56 }}>
              <div style={{
                display:"inline-block",
                background:"linear-gradient(135deg,#4dabf7,#cc5de8)",
                WebkitBackgroundClip:"text", WebkitTextFillColor:"transparent",
                fontSize:48, fontWeight:800, letterSpacing:-2, lineHeight:1.1,
                marginBottom:16,
              }}>
                Database Normalization<br/>Made Automatic
              </div>
              <div style={{ color:"#566a7f", fontSize:15, maxWidth:520, margin:"0 auto" }}>
                Upload any Excel file. Our engine analyzes functional dependencies
                and applies all five normal forms — NF1 through NF5 — automatically.
              </div>
            </div>

            {/* NF pipeline visual */}
            <div style={{
              background:"#0a1520",
              border:"1px solid #1a2a3a",
              borderRadius:14,
              padding:"24px 32px",
              marginBottom:40,
              display:"flex", alignItems:"center", gap:24, flexWrap:"wrap",
            }}>
              <div style={{ color:"#566a7f", fontSize:12, fontFamily:"'JetBrains Mono', monospace" }}>
                PIPELINE
              </div>
              <NfPipeline completedSteps={[]} />
            </div>

            {/* Drop zone */}
            <DropZone onFile={handleUpload} disabled={false} />

            {/* Feature grid */}
            <div style={{
              display:"grid", gridTemplateColumns:"repeat(auto-fit, minmax(200px,1fr))",
              gap:16, marginTop:40,
            }}>
              {[
                { icon:"⚛️",  title:"Atomic Cells",     desc:"Splits comma-separated values into proper rows (NF1)" },
                { icon:"🔗",  title:"Dependency Graph",  desc:"Detects all functional and multi-valued dependencies" },
                { icon:"🗂️", title:"Table Extraction",  desc:"Creates separate tables for each logical entity" },
                { icon:"💾",  title:"SQL Export",        desc:"Generates ready-to-run MySQL CREATE TABLE + INSERTs" },
              ].map(f => (
                <div key={f.title} style={{
                  background:"#0a1520", border:"1px solid #1a2a3a",
                  borderRadius:10, padding:"20px 18px",
                }}>
                  <div style={{ fontSize:24, marginBottom:10 }}>{f.icon}</div>
                  <div style={{ fontWeight:700, marginBottom:6, fontSize:13 }}>{f.title}</div>
                  <div style={{ color:"#566a7f", fontSize:12, lineHeight:1.6 }}>{f.desc}</div>
                </div>
              ))}
            </div>
          </div>
        )}

        {/* ════ PROCESSING + DONE STATES ════ */}
        {status !== "idle" && (
          <div className="fade-in">
            {/* File info banner */}
            <div style={{
              background:"#0a1520", border:"1px solid #1a2a3a",
              borderRadius:12, padding:"16px 24px",
              display:"flex", alignItems:"center", gap:16, marginBottom:24,
              flexWrap:"wrap",
            }}>
              <div style={{ fontSize:28 }}>📄</div>
              <div>
                <div style={{ fontWeight:700, fontSize:14 }}>{file?.name}</div>
                <div style={{ color:"#566a7f", fontSize:12, fontFamily:"'JetBrains Mono', monospace" }}>
                  {(file?.size / 1024).toFixed(0)} KB
                </div>
              </div>
              <div style={{ marginLeft:"auto", display:"flex", alignItems:"center", gap:16 }}>
                {isProcessing && (
                  <div style={{ display:"flex", alignItems:"center", gap:8, color:"#ffd43b" }}>
                    <div style={{
                      width:16, height:16, border:"2px solid #ffd43b",
                      borderTopColor:"transparent", borderRadius:"50%",
                      animation:"spin 0.8s linear infinite",
                    }}/>
                    <span style={{ fontSize:12, fontFamily:"'JetBrains Mono', monospace" }}>
                      Processing…
                    </span>
                  </div>
                )}
                {status === "done" && (
                  <div style={{ color:"#69db7c", display:"flex", alignItems:"center", gap:8 }}>
                    <span style={{ fontSize:18 }}>✅</span>
                    <span style={{ fontSize:13, fontWeight:700 }}>
                      {tables.length} tables · Ready to download
                    </span>
                  </div>
                )}
                {status === "error" && (
                  <div style={{ color:"#ff6b6b", fontSize:13, fontWeight:700 }}>
                    ❌ {errorMsg}
                  </div>
                )}
              </div>
            </div>

            {/* NF Pipeline progress */}
            <div style={{
              background:"#0a1520", border:"1px solid #1a2a3a",
              borderRadius:12, padding:"16px 24px", marginBottom:24,
              display:"flex", alignItems:"center", gap:20, flexWrap:"wrap",
            }}>
              <span style={{ color:"#566a7f", fontSize:11,
                fontFamily:"'JetBrains Mono', monospace", whiteSpace:"nowrap" }}>
                NF PROGRESS
              </span>
              <NfPipeline completedSteps={
                status === "done"
                  ? NF_ORDER
                  : logs.reduce((acc, l) => {
                      NF_ORDER.forEach(nf => {
                        const n = nf.replace("NF","");
                        if ((l.includes(`[NF${n}]`) || l.includes(`NF${n} `)) && !acc.includes(nf))
                          acc.push(nf);
                      });
                      return acc;
                    }, [])
              }/>
            </div>

            {/* Two-column layout: terminal + content */}
            <div style={{ display:"grid", gridTemplateColumns:"1fr 1.8fr", gap:20, alignItems:"start" }}>

              {/* Terminal */}
              <div>
                <div style={{ color:"#566a7f", fontSize:11,
                  fontFamily:"'JetBrains Mono', monospace", marginBottom:8 }}>
                  LIVE LOG
                </div>
                <Terminal logs={logs} status={status} />

                {status === "done" && (
                  <div style={{ marginTop:12, display:"flex", flexDirection:"column", gap:8 }}>
                    <a href={`${API}/download/${jobId}/excel`} download style={{ textDecoration:"none" }}>
                      <button className="btn-primary" style={{ width:"100%" }}>
                        ⬇ Download Normalized Excel
                      </button>
                    </a>
                    <a href={`${API}/download/${jobId}/sql`} download style={{ textDecoration:"none" }}>
                      <button className="btn-ghost" style={{ width:"100%" }}>
                        ⬇ Download SQL Schema
                      </button>
                    </a>
                  </div>
                )}
              </div>

              {/* Results panel */}
              <div>
                {/* Tabs */}
                <div style={{
                  display:"flex", borderBottom:"1px solid #1a2a3a", marginBottom:20
                }}>
                  {[
                    { id:"tables", label:"Tables" },
                    { id:"preview", label:"Data Preview" },
                    { id:"schema",  label:"Schema Map" },
                  ].map(t => (
                    <button key={t.id} style={tabStyle(t.id)}
                      onClick={() => setSelectedTab(t.id)}>
                      {t.label}
                    </button>
                  ))}
                </div>

                {/* Tables tab */}
                {selectedTab === "tables" && (
                  <div>
                    {isProcessing && tables.length === 0 && (
                      <div style={{ color:"#2a3a4a", fontSize:13, padding:"40px 0", textAlign:"center" }}>
                        Tables will appear here once normalization completes…
                      </div>
                    )}
                    <div style={{
                      display:"grid", gridTemplateColumns:"repeat(auto-fill,minmax(220px,1fr))",
                      gap:12,
                    }}>
                      {tables.map(t => (
                        <TableCard
                          key={t.name} table={t}
                          isSelected={selectedTbl?.name === t.name}
                          onClick={() => { setSelectedTbl(t); setSelectedTab("preview"); }}
                        />
                      ))}
                    </div>

                    {/* Stats row */}
                    {tables.length > 0 && (
                      <div style={{
                        marginTop:20, display:"flex", gap:24, flexWrap:"wrap",
                        padding:"16px 20px", background:"#070d12",
                        border:"1px solid #0d1a24", borderRadius:10,
                      }}>
                        {[
                          { label:"Tables",   val: tables.length },
                          { label:"Total rows", val: tables.reduce((s,t)=>s+t.rows,0) },
                          { label:"Columns",  val: tables.reduce((s,t)=>s+t.columns.length,0) },
                          { label:"NF level", val: "5NF" },
                        ].map(s => (
                          <div key={s.label}>
                            <div style={{ color:"#4dabf7", fontSize:22, fontWeight:800,
                              fontFamily:"'JetBrains Mono', monospace" }}>
                              {s.val}
                            </div>
                            <div style={{ color:"#566a7f", fontSize:11 }}>{s.label}</div>
                          </div>
                        ))}
                      </div>
                    )}
                  </div>
                )}

                {/* Preview tab */}
                {selectedTab === "preview" && (
                  <div>
                    {/* Table selector */}
                    <div style={{ display:"flex", gap:8, flexWrap:"wrap", marginBottom:16 }}>
                      {tables.map(t => (
                        <button
                          key={t.name}
                          onClick={() => setSelectedTbl(t)}
                          style={{
                            padding:"5px 12px", borderRadius:6, cursor:"pointer",
                            fontSize:11, fontFamily:"'JetBrains Mono', monospace",
                            fontWeight:700, border:`1px solid ${selectedTbl?.name===t.name ? getNfColor(t.nf) : "#1a2a3a"}`,
                            background: selectedTbl?.name===t.name ? getNfColor(t.nf)+"22" : "transparent",
                            color: selectedTbl?.name===t.name ? getNfColor(t.nf) : "#566a7f",
                            transition:"all .15s",
                          }}
                        >
                          {t.name}
                        </button>
                      ))}
                    </div>

                    {selectedTbl ? (
                      <div style={{
                        background:"#070d12", border:"1px solid #1a2a3a",
                        borderRadius:10, overflow:"hidden",
                      }}>
                        <div style={{
                          padding:"10px 16px", borderBottom:"1px solid #1a2a3a",
                          display:"flex", alignItems:"center", gap:12,
                        }}>
                          <span style={{ fontWeight:700, fontSize:13,
                            fontFamily:"'JetBrains Mono', monospace" }}>
                            {selectedTbl.name}
                          </span>
                          <NfBadge nf={selectedTbl.nf} />
                          <span style={{ color:"#566a7f", fontSize:11 }}>
                            {selectedTbl.rows} rows · {selectedTbl.columns.length} cols
                          </span>
                        </div>
                        <DataPreview table={selectedTbl} />
                      </div>
                    ) : (
                      <div style={{ color:"#2a3a4a", fontSize:13, padding:"40px 0", textAlign:"center" }}>
                        Select a table above to preview data
                      </div>
                    )}
                  </div>
                )}

                {/* Schema tab */}
                {selectedTab === "schema" && (
                  <div>
                    {tables.length > 0
                      ? <SchemaMap tables={tables} />
                      : <div style={{ color:"#2a3a4a", fontSize:13, padding:"40px 0", textAlign:"center" }}>
                          Schema will appear after normalization…
                        </div>
                    }
                  </div>
                )}
              </div>
            </div>
          </div>
        )}
      </div>
    </div>
  );
}
