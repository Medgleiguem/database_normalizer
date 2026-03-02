"""
╔══════════════════════════════════════════════════════════════════╗
║  GROQ AI ADVISOR  —  Semantic analysis for NormalizerDB          ║
║  Uses LLaMA 3.3-70B via Groq free API                           ║
╚══════════════════════════════════════════════════════════════════╝

Replaces pure heuristics with AI-powered understanding:
  • Identifies functional dependencies from column semantics
  • Detects composite attributes (e.g. "full_name" → first + last)
  • Flags calculated/derived columns (mfe, total, decision...)
  • Detects multi-valued columns and groups parallel ones
  • Suggests meaningful table names for each entity
  • Produces a human-readable normalization plan
"""

import json
import re
import urllib.request
import urllib.error
import pandas as pd

GROQ_API_URL = "https://api.groq.com/openai/v1/chat/completions"
MODEL         = "llama-3.3-70b-versatile"   # best free Groq model
FAST_MODEL    = "llama-3.1-8b-instant"      # fallback for quick calls


# ── Core HTTP call (no external deps beyond stdlib) ──────────────

def _groq_call(api_key: str, messages: list, model: str = MODEL,
               temperature: float = 0.0, max_tokens: int = 4096) -> str:
    """
    Raw Groq API call using only urllib (no openai/httpx needed).
    Returns the assistant message text.
    Raises RuntimeError on API or network error.
    """
    payload = json.dumps({
        "model":       model,
        "messages":    messages,
        "temperature": temperature,
        "max_tokens":  max_tokens,
        "stream":      False,
    }).encode("utf-8")

    req = urllib.request.Request(
        GROQ_API_URL,
        data=payload,
        headers={
            "Content-Type":  "application/json",
            "Authorization": f"Bearer {api_key}",
        },
        method="POST",
    )

    try:
        with urllib.request.urlopen(req, timeout=60) as resp:
            body = json.loads(resp.read().decode("utf-8"))
            return body["choices"][0]["message"]["content"]
    except urllib.error.HTTPError as e:
        err_body = e.read().decode("utf-8", errors="replace")
        raise RuntimeError(f"Groq API error {e.code}: {err_body}")
    except urllib.error.URLError as e:
        raise RuntimeError(f"Network error calling Groq: {e.reason}")


def _extract_json(text: str) -> dict:
    """Pull the first JSON object out of a (possibly noisy) LLM response."""
    # Strip markdown code fences
    text = re.sub(r"```(?:json)?", "", text).strip()
    # Find outermost { ... }
    start = text.find("{")
    end   = text.rfind("}") + 1
    if start == -1 or end == 0:
        raise ValueError(f"No JSON object found in response:\n{text[:500]}")
    return json.loads(text[start:end])


# ── Build a compact table summary for the prompt ─────────────────

def _table_summary(df: pd.DataFrame, max_sample: int = 5) -> str:
    """
    Produce a compact text snapshot of the dataframe:
      Columns (dtype): sample_val1 | sample_val2 | ...
    """
    lines = [f"Table has {len(df)} rows and {len(df.columns)} columns.\n"]
    for col in df.columns:
        dtype = str(df[col].dtype)
        samples = (
            df[col]
            .dropna()
            .astype(str)
            .head(max_sample)
            .tolist()
        )
        sample_str = " | ".join(samples)
        lines.append(f"  {col!r:40s} ({dtype}): {sample_str}")
    return "\n".join(lines)


# ── Main analysis call ────────────────────────────────────────────

SYSTEM_PROMPT = """You are a senior database architect expert in relational theory and SQL normalization (1NF through 5NF).
You will receive a description of a flat/denormalized database table and must return a structured JSON analysis.
Return ONLY valid JSON — no markdown, no explanation text, no preamble."""

ANALYSIS_PROMPT = """Analyze this denormalized database table and return a JSON object with exactly these keys:

{
  "table_domain": "<one sentence describing what this table is about>",
  "primary_key": ["<col>"],
  "composite_columns": {
    "<col_name>": ["<sub_attr1>", "<sub_attr2>"]
  },
  "multi_valued_columns": ["<col>"],
  "parallel_groups": [["<col_a>", "<col_b>"]],
  "independent_mvd_columns": ["<col>"],
  "functional_dependencies": {
    "<determinant_col>": ["<dependent_col1>", "<dependent_col2>"]
  },
  "calculated_columns": ["<col>"],
  "entity_tables": [
    {
      "name": "<EntityName>",
      "description": "<what this entity represents>",
      "columns": ["<col1>", "<col2>"],
      "primary_key": ["<col>"],
      "foreign_keys": ["<col>"]
    }
  ],
  "normalization_notes": "<plain-text summary of violations found and steps needed>"
}

Rules:
- primary_key: the minimal set of columns that uniquely identifies each row
- composite_columns: columns whose NAME or VALUES combine multiple facts (e.g. "full_name" contains first+last, "address" combines street+city)
- multi_valued_columns: columns that contain comma/semicolon separated lists of values (e.g. "MA, MAI" or "L1, L2, L3")
- parallel_groups: lists of multi-valued columns that always have the same number of values per row (they should be exploded together)
- independent_mvd_columns: multi-valued columns that are independent facts (should get their own bridge table for NF4)
- functional_dependencies: only non-trivial FDs where the determinant is NOT the full primary key
- calculated_columns: columns whose value is derived/computed from other columns (averages, totals, grades, decisions)
- entity_tables: the normalized tables this flat table should be decomposed into, one per logical entity
- Use the actual column names from the table exactly as given

TABLE DESCRIPTION:
%s"""


def analyze_table(df: pd.DataFrame, api_key: str,
                  log_fn=None) -> dict:
    """
    Call Groq to semantically analyze the table.
    Returns the parsed JSON dict, or {} on failure (falls back to heuristics).

    log_fn: optional callable(str) for streaming log messages to the frontend.
    """
    def _log(msg):
        if log_fn:
            log_fn(msg)
        print(f"    [Groq] {msg}")

    _log("🤖 Sending table to Groq LLaMA 3.3-70B for semantic analysis…")

    summary = _table_summary(df)
    prompt  = ANALYSIS_PROMPT % summary

    try:
        raw = _groq_call(api_key, [
            {"role": "system",  "content": SYSTEM_PROMPT},
            {"role": "user",    "content": prompt},
        ])
        result = _extract_json(raw)
        _log(f"✅ Groq analysis received — domain: {result.get('table_domain', '?')}")
        _log(f"   PK suggested: {result.get('primary_key', [])}")
        _log(f"   Entity tables: {[t['name'] for t in result.get('entity_tables', [])]}")
        if result.get("normalization_notes"):
            _log(f"   Notes: {result['normalization_notes'][:180]}")
        return result
    except Exception as e:
        _log(f"⚠️  Groq call failed ({e}) — falling back to heuristics")
        return {}


# ── Groq-enhanced SQL name suggestions ───────────────────────────

def suggest_table_names(tables: dict, api_key: str,
                        log_fn=None) -> dict:
    """
    Given the final normalized tables dict, ask Groq to suggest
    better semantic names for each table.
    Returns {old_name: suggested_name} mapping.
    """
    def _log(msg):
        if log_fn: log_fn(msg)

    _log("🤖 Asking Groq to suggest semantic table names…")

    table_info = []
    for tname, (df, pk, fk) in tables.items():
        table_info.append({
            "current_name": tname,
            "columns":      list(df.columns),
            "pk":           pk,
        })

    prompt = (
        "Given these normalized database tables, suggest a clean, meaningful SQL table name "
        "for each one. Return ONLY a JSON object mapping current_name → suggested_name.\n\n"
        + json.dumps(table_info, indent=2)
    )

    try:
        raw = _groq_call(api_key, [
            {"role": "system",  "content": "You are a database naming expert. Return only valid JSON."},
            {"role": "user",    "content": prompt},
        ], model=FAST_MODEL, max_tokens=512)
        mapping = _extract_json(raw)
        _log(f"✅ Name suggestions received: {mapping}")
        return mapping
    except Exception as e:
        _log(f"⚠️  Name suggestion failed ({e}) — keeping auto-generated names")
        return {}


# ── Groq-generated SQL comments ──────────────────────────────────

def generate_sql_comments(tables: dict, domain: str,
                          api_key: str, log_fn=None) -> dict:
    """
    Ask Groq to generate a one-line SQL COMMENT for each table.
    Returns {table_name: comment_string}
    """
    def _log(msg):
        if log_fn: log_fn(msg)

    table_summaries = [
        f"{tname}: columns={list(df.columns)}, pk={pk}"
        for tname, (df, pk, fk) in tables.items()
    ]

    prompt = (
        f"Database domain: {domain}\n"
        "Write a one-line SQL COMMENT (max 80 chars) for each of these normalized tables. "
        "Return ONLY a JSON object: {table_name: comment}.\n\n"
        + "\n".join(table_summaries)
    )

    try:
        raw = _groq_call(api_key, [
            {"role": "system",  "content": "You are a database documentation expert. Return only valid JSON."},
            {"role": "user",    "content": prompt},
        ], model=FAST_MODEL, max_tokens=512)
        return _extract_json(raw)
    except Exception:
        return {}


# ── Merge Groq analysis with heuristic results ────────────────────

def merge_analysis(groq_result: dict, heuristic_fds: dict,
                   heuristic_mv: list, heuristic_composites: dict,
                   heuristic_calc: list) -> dict:
    """
    Merge Groq's semantic analysis with the heuristic fallbacks.
    Groq results take precedence where they exist.
    Returns a unified analysis dict used by the engine.
    """
    if not groq_result:
        return {
            "primary_key":            None,
            "composite_columns":      heuristic_composites,
            "multi_valued_columns":   heuristic_mv,
            "parallel_groups":        [],
            "independent_mvd_columns": [],
            "functional_dependencies": heuristic_fds,
            "calculated_columns":     heuristic_calc,
            "entity_tables":          [],
            "table_domain":           "Unknown",
        }

    # Merge FDs: Groq FDs override, heuristic fills gaps
    merged_fds = dict(heuristic_fds)
    for det, deps in groq_result.get("functional_dependencies", {}).items():
        merged_fds[det] = list(set(merged_fds.get(det, []) + deps))

    # Merge multi-valued
    groq_mv    = groq_result.get("multi_valued_columns", [])
    merged_mv  = list(set(heuristic_mv + groq_mv))

    # Merge calculated columns
    groq_calc  = groq_result.get("calculated_columns", [])
    merged_calc = list(set(heuristic_calc + groq_calc))

    # Merge composite columns
    groq_comp  = groq_result.get("composite_columns", {})
    merged_comp = dict(heuristic_composites)
    merged_comp.update(groq_comp)

    return {
        "primary_key":             groq_result.get("primary_key"),
        "composite_columns":       merged_comp,
        "multi_valued_columns":    merged_mv,
        "parallel_groups":         groq_result.get("parallel_groups", []),
        "independent_mvd_columns": groq_result.get("independent_mvd_columns", []),
        "functional_dependencies": merged_fds,
        "calculated_columns":      merged_calc,
        "entity_tables":           groq_result.get("entity_tables", []),
        "table_domain":            groq_result.get("table_domain", "Unknown"),
        "normalization_notes":     groq_result.get("normalization_notes", ""),
    }
