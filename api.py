# app.py
#!/usr/bin/env python3
from fastapi import FastAPI, HTTPException
from fastapi.responses import JSONResponse
from datetime import datetime
from zoneinfo import ZoneInfo
from io import StringIO
from contextlib import redirect_stdout, redirect_stderr
import re

app = FastAPI()

DATE_RE = re.compile(r"\b(\d{4}-\d{2}-\d{2})\b")

def extract_date_from_any(s: str) -> str:
    matches = DATE_RE.findall(s or "")
    if not matches:
        raise HTTPException(status_code=400, detail="No YYYY-mm-dd date found in the URI segment")
    for token in reversed(matches):
        try:
            datetime.strptime(token, "%Y-%m-%d")
            return token
        except ValueError:
            continue
    raise HTTPException(status_code=400, detail="Found date-like text, but not a valid YYYY-mm-dd")

@app.get("/si-log-extract/{anything}")
def si_log_extract(anything: str):
    # 1) get the date and build the timestamp suffix (yyyy-mm-dd-HHMMSS in IST)
    date_str = extract_date_from_any(anything)
    ts = f"{date_str}-{datetime.now(ZoneInfo('Asia/Kolkata')).strftime('%H%M%S')}"

    # 2) run main.main(ts) and capture output for debugging
    out_buf, err_buf = StringIO(), StringIO()
    try:
        import os
        from pathlib import Path
        import main as main_mod

        with redirect_stdout(out_buf), redirect_stderr(err_buf):
            paths = main_mod.main(ts)  # may be None if main() doesn't return

        # ---- Fallback: if main() didn't return paths, call process() directly ----
        if not isinstance(paths, dict) or not paths:
            cfg_path_str = main_mod.expand_env_str(main_mod.CONFIG_TEMPLATE)
            if "${ROOT_PATH}" in main_mod.CONFIG_TEMPLATE or "$ROOT_PATH" in main_mod.CONFIG_TEMPLATE:
                root = os.environ.get("ROOT_PATH", ".")
                cfg_path_str = str(Path(root) / "config.json")

            cfg_path = Path(cfg_path_str).resolve()
            if not cfg_path.exists():
                raise HTTPException(status_code=500, detail={
                    "error": f"Config not found at: {cfg_path}",
                    "stdout": out_buf.getvalue(),
                    "stderr": err_buf.getvalue(),
                })

            with redirect_stdout(out_buf), redirect_stderr(err_buf):
                stats = main_mod.process(cfg_path, input_date=ts)

            paths = (stats or {}).get("paths", {})

    except SystemExit as se:
        code = se.code if isinstance(se.code, int) else 1
        if code != 0:
            raise HTTPException(status_code=500, detail={
                "error": "main.py exited with non-zero status",
                "code": code,
                "stdout": out_buf.getvalue(),
                "stderr": err_buf.getvalue(),
            })
        paths = None
    except Exception as e:
        raise HTTPException(status_code=500, detail={
            "error": f"Failed running main.py: {e}",
            "stdout": out_buf.getvalue(),
            "stderr": err_buf.getvalue(),
        })

    # 3) return whatever we have (ensure 'date' is present)
    if isinstance(paths, dict) and paths:
        paths.setdefault("date", ts)
        return JSONResponse(content=paths)

    # Fallback if nothing usable
    return JSONResponse(content={"date": ts})
