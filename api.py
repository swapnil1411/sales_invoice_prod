#!/usr/bin/env python3
from fastapi import FastAPI, HTTPException
from fastapi.responses import JSONResponse
from datetime import datetime
from zoneinfo import ZoneInfo
from io import StringIO
from contextlib import redirect_stdout, redirect_stderr
import os, re

# ---- Make sibling imports robust (so gcs_utils/main import regardless of CWD) ----
from pathlib import Path as _Path
import sys as _sys
_SYS_THIS_DIR = str(_Path(__file__).resolve().parent)
if _SYS_THIS_DIR not in _sys.path:
    _sys.path.insert(0, _SYS_THIS_DIR)

from gcs_utils import run_with_optional_gcs  # GCS bridge only (no local)
import main as main_mod  # to reuse process()

app = FastAPI()

DATE_RE = re.compile(r"\b(\d{4}-\d{2}-\d{2})\b")

def extract_date_from_any(s: str) -> str:
    m = DATE_RE.findall(s or "")
    if not m:
        raise HTTPException(status_code=400, detail="No YYYY-mm-dd date found in the URI segment")
    for token in reversed(m):
        try:
            datetime.strptime(token, "%Y-%m-%d")
            return token
        except ValueError:
            continue
    raise HTTPException(status_code=400, detail="Found date-like text, but not a valid YYYY-mm-dd")

@app.get("/si-log-extract/{anything}")
def si_log_extract(anything: str):
    # 1) Parse date and build IST timestamp suffix yyyy-mm-dd-HHMMSS
    date_str = extract_date_from_any(anything)
    ts = f"{date_str}-{datetime.now(ZoneInfo('Asia/Kolkata')).strftime('%H%M%S')}"

    # 2) Require gs:// ROOT_PATH
    root = (os.environ.get("ROOT_PATH", "") or "").strip()
    if not root.startswith("gs://"):
        raise HTTPException(
            status_code=400,
            detail="ROOT_PATH must start with 'gs://'. Set e.g. export ROOT_PATH='gs://bucket/prefix'"
        )
    print("[INFO] ROOT_PATH:", root)
    cfg_path_str = root.rstrip("/") + "/config.json"
    print(f"[INFO] Using config path: {cfg_path_str}")
    # 3) Run via the GCS bridge only; capture stdout/stderr for debugging
    out_buf, err_buf = StringIO(), StringIO()
    try:
        with redirect_stdout(out_buf), redirect_stderr(err_buf):
            stats = run_with_optional_gcs(
                config_path_str=cfg_path_str,
                process_fn=lambda local_cfg_path: main_mod.process(local_cfg_path, input_date=ts),
            )
    except Exception as e:
        raise HTTPException(status_code=500, detail={
            "error": f"GCS run failed: {e}",
            "stdout": out_buf.getvalue(),
            "stderr": err_buf.getvalue(),
        })

    # 4) Response — prefer returning the whole stats dict (includes paths + counters)
    if not isinstance(stats, dict) or not stats.get("paths"):
        # Return what we have with timestamp so caller knows the run ID
        return JSONResponse(content={"date": ts, "stats": stats or {}, "note": "No paths generated"})

    # ensure the timestamp is visible to caller
    stats.setdefault("date", ts)
    return JSONResponse(content=stats)
