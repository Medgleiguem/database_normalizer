"""
Flask Backend — NormalizerDB v2.0 + Groq AI
"""
import os, uuid, json, time, threading, traceback
from pathlib import Path
from flask import Flask, request, jsonify, send_file, Response, stream_with_context
import pandas as pd

app = Flask(__name__)

@app.after_request
def add_cors(response):
    response.headers["Access-Control-Allow-Origin"]  = "*"
    response.headers["Access-Control-Allow-Headers"] = "Content-Type,Authorization"
    response.headers["Access-Control-Allow-Methods"] = "GET,POST,OPTIONS"
    return response

@app.route("/api/<path:p>", methods=["OPTIONS"])
def options_handler(p): return "", 204

UPLOAD_DIR = Path("./uploads"); UPLOAD_DIR.mkdir(exist_ok=True)
OUTPUT_DIR = Path("./outputs"); OUTPUT_DIR.mkdir(exist_ok=True)
MAX_FILE_MB = 20

JOBS: dict = {}
LOGS: dict = {}


def job_log(job_id, msg):
    LOGS.setdefault(job_id, []).append(msg)
    JOBS[job_id]["last_log"] = msg


def _nf_label(tname):
    t = tname.lower()
    if "5nf" in t: return "5NF"
    if "mvd" in t: return "4NF"
    if "bcnf" in t: return "BCNF"
    if "ref"  in t: return "3NF"
    if "table" in t: return "2NF"
    return "5NF"


def run_normalization(job_id, input_path, groq_api_key=None):
    try:
        JOBS[job_id]["status"] = "running"
        job_log(job_id, "📂 File received — reading Excel…")
        time.sleep(0.2)

        out_excel = str(OUTPUT_DIR / f"{job_id}_normalized.xlsx")
        out_sql   = str(OUTPUT_DIR / f"{job_id}_schema.sql")

        import normalize_engine as eng
        import importlib; importlib.reload(eng)

        # Detect sheets
        xl = pd.ExcelFile(input_path)
        sheets = xl.sheet_names
        job_log(job_id, f"📊 Sheets found: {sheets} — processing '{sheets[0]}'")
        time.sleep(0.15)

        if groq_api_key:
            job_log(job_id, f"🤖 Groq AI mode enabled — model: llama-3.3-70b-versatile")
        else:
            job_log(job_id, "⚙️  Heuristic mode (no Groq key provided)")

        # Patch engine functions to stream logs via SSE
        _originals = {
            'nf1': eng.apply_nf1, 'nf2': eng.apply_nf2,
            'nf3': eng.apply_nf3, 'bcnf': eng.apply_bcnf,
            'nf4': eng.apply_nf4, 'nf5': eng.apply_nf5,
        }

        def make_patched(fn_key, label):
            def patched(*args, **kwargs):
                job_log(job_id, f"🔎 {label}")
                result = _originals[fn_key](*args, **kwargs)
                # Stream the log list (always the second return value)
                logs = result[1] if isinstance(result, tuple) else []
                for m in logs: job_log(job_id, f"  {m}")
                return result
            return patched

        eng.apply_nf1  = make_patched('nf1',  "NF1 — Atomic values, composite attrs, repeating groups…")
        eng.apply_nf2  = make_patched('nf2',  "NF2 — Partial dependency detection…")
        eng.apply_nf3  = make_patched('nf3',  "NF3 — Transitive deps & calculated fields…")
        eng.apply_bcnf = make_patched('bcnf', "BCNF — Superkey verification…")
        eng.apply_nf4  = make_patched('nf4',  "NF4 — Multi-valued dependency check…")
        eng.apply_nf5  = make_patched('nf5',  "NF5 — Join dependency check…")

        # Also stream Groq advisor logs
        def groq_log(msg): job_log(job_id, msg)

        tables, nf_log = eng.normalize(
            input_excel=input_path,
            output_excel=out_excel,
            output_sql=out_sql,
            sheet_name=0,
            groq_api_key=groq_api_key,
            log_fn=groq_log,
        )

        # Build metadata for frontend
        tables_meta = []
        for tname, (df, pk, fk) in tables.items():
            import numpy as np
            preview_df = df.head(5).copy()
            for col in preview_df.columns:
                preview_df[col] = preview_df[col].apply(
                    lambda v: None if (isinstance(v, float) and np.isnan(v)) else
                              int(v) if isinstance(v, np.integer) else
                              float(v) if isinstance(v, np.floating) else v
                )
            tables_meta.append({
                "name":    tname,
                "rows":    len(df),
                "columns": list(df.columns),
                "pk":      pk,
                "fk":      fk,
                "nf":      _nf_label(tname),
                "preview": preview_df.fillna("").astype(str).to_dict(orient="records"),
            })

        JOBS[job_id].update({
            "status":      "done",
            "tables":      tables_meta,
            "nf_log":      {k: v for k, v in nf_log.items()},
            "excel_path":  out_excel,
            "sql_path":    out_sql,
            "table_count": len(tables_meta),
            "ai_mode":     bool(groq_api_key),
        })
        job_log(job_id, f"✅ Done! {len(tables_meta)} normalized tables.")

    except Exception as e:
        JOBS[job_id]["status"] = "error"
        JOBS[job_id]["error"]  = str(e)
        job_log(job_id, f"❌ Error: {e}")
        for line in traceback.format_exc().split('\n'):
            if line.strip(): job_log(job_id, f"  {line}")


# ── Validate Groq key ────────────────────────────────────────────

@app.route("/api/validate-groq", methods=["POST"])
def validate_groq():
    data = request.get_json() or {}
    key  = data.get("api_key", "").strip()
    if not key:
        return jsonify({"valid": False, "error": "No key provided"}), 400
    try:
        import urllib.request, json as _json
        payload = _json.dumps({
            "model": "llama-3.1-8b-instant",
            "messages": [{"role": "user", "content": "Reply with just: OK"}],
            "max_tokens": 5,
        }).encode()
        req = urllib.request.Request(
            "https://api.groq.com/openai/v1/chat/completions",
            data=payload,
            headers={"Content-Type": "application/json",
                     "Authorization": f"Bearer {key}"},
            method="POST",
        )
        with urllib.request.urlopen(req, timeout=10) as r:
            body = _json.loads(r.read())
            reply = body["choices"][0]["message"]["content"]
            return jsonify({"valid": True, "reply": reply})
    except urllib.error.HTTPError as e:
        err = e.read().decode()
        return jsonify({"valid": False, "error": f"HTTP {e.code}: {err}"}), 400
    except Exception as e:
        return jsonify({"valid": False, "error": str(e)}), 400


# ── Routes ───────────────────────────────────────────────────────

@app.route("/api/health")
def health():
    return jsonify({"ok": True, "version": "2.0.0"})


@app.route("/api/normalize", methods=["POST"])
def normalize():
    if "file" not in request.files:
        return jsonify({"error": "No file uploaded"}), 400
    f = request.files["file"]
    if not f.filename.lower().endswith((".xlsx", ".xls")):
        return jsonify({"error": "Only .xlsx / .xls files accepted"}), 400
    f.seek(0, 2); size_mb = f.tell() / (1024*1024); f.seek(0)
    if size_mb > MAX_FILE_MB:
        return jsonify({"error": f"File too large ({size_mb:.1f} MB). Max {MAX_FILE_MB} MB."}), 400

    groq_key  = request.form.get("groq_api_key", "").strip() or None
    job_id    = str(uuid.uuid4())[:8]
    safe_name = f.filename.replace(" ","_").replace("/","_")
    save_path = str(UPLOAD_DIR / f"{job_id}_{safe_name}")
    f.save(save_path)

    JOBS[job_id] = {
        "status":     "queued",
        "filename":   f.filename,
        "size_mb":    round(size_mb, 2),
        "created_at": time.time(),
        "ai_mode":    bool(groq_key),
    }
    LOGS[job_id] = []

    t = threading.Thread(target=run_normalization,
                         args=(job_id, save_path, groq_key))
    t.daemon = True; t.start()
    return jsonify({"job_id": job_id}), 202


@app.route("/api/jobs/<job_id>")
def get_job(job_id):
    if job_id not in JOBS:
        return jsonify({"error": "Job not found"}), 404
    job = dict(JOBS[job_id])
    job["logs"] = LOGS.get(job_id, [])
    job.pop("excel_path", None); job.pop("sql_path", None)
    return jsonify(job)


@app.route("/api/stream/<job_id>")
def stream_logs(job_id):
    if job_id not in JOBS:
        return jsonify({"error": "Job not found"}), 404
    def generate():
        sent = 0
        while True:
            logs = LOGS.get(job_id, [])
            while sent < len(logs):
                msg = logs[sent].replace("\n", " ")
                yield f"data: {json.dumps({'log': msg, 'idx': sent})}\n\n"
                sent += 1
            if JOBS[job_id].get("status") in ("done", "error"):
                yield f"data: {json.dumps({'status': JOBS[job_id]['status'], 'done': True})}\n\n"
                break
            time.sleep(0.15)
    return Response(stream_with_context(generate()),
                    mimetype="text/event-stream",
                    headers={"Cache-Control":"no-cache","X-Accel-Buffering":"no"})


@app.route("/api/download/<job_id>/excel")
def download_excel(job_id):
    if job_id not in JOBS or JOBS[job_id].get("status") != "done":
        return jsonify({"error": "Not ready"}), 404
    return send_file(JOBS[job_id]["excel_path"], as_attachment=True,
                     download_name=f"normalized_{job_id}.xlsx",
                     mimetype="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")


@app.route("/api/download/<job_id>/sql")
def download_sql(job_id):
    if job_id not in JOBS or JOBS[job_id].get("status") != "done":
        return jsonify({"error": "Not ready"}), 404
    return send_file(JOBS[job_id]["sql_path"], as_attachment=True,
                     download_name=f"schema_{job_id}.sql",
                     mimetype="application/sql")


if __name__ == "__main__":
    app.run(debug=True, port=5000, threaded=True)
