# app.py
#!/usr/bin/env python3
from fastapi import FastAPI, HTTPException
from fastapi.responses import JSONResponse
from datetime import datetime
from zoneinfo import ZoneInfo
import re

app = FastAPI()

# Find any YYYY-mm-dd substring within the path piece
DATE_RE = re.compile(r"\b(\d{4}-\d{2}-\d{2})\b")

def extract_date_from_any(s: str) -> str:
    """
    Return the LAST valid YYYY-mm-dd found in s.
    Raises HTTP 400 if none found or none are valid dates.
    """
    matches = DATE_RE.findall(s or "")
    if not matches:
        raise HTTPException(status_code=400, detail="No YYYY-mm-dd date found in the URI segment")

    # Try from the end (last date wins)
    for token in reversed(matches):
        try:
            # Validate calendar date
            datetime.strptime(token, "%Y-%m-%d")
            return token
        except ValueError:
            continue

    raise HTTPException(status_code=400, detail="Found date-like text, but not a valid YYYY-mm-dd")

@app.get("/si-log-extract/{anything}")
def si_log_extract(anything: str):
    # Extract and validate date from arbitrary string
    date_str = extract_date_from_any(anything)

    # IST timestamp suffix: yyyy-mm-dd-HHMMSS
    ts = f"{date_str}-{datetime.now(ZoneInfo('Asia/Kolkata')).strftime('%H%M%S')}"

    input_prefix = f"salesinvoice/producer-input/{ts}"
    output_root  = f"salesinvoice/expected-output/{ts}"

    resp = {
        "input": input_prefix,
        "mirkl_output": f"{output_root}/mirkl",
        "vertex_output": f"{output_root}/vertex",
        "ip-us": f"{output_root}/ip-us",
        "ip-uk": f"{output_root}/ip-uk",
        "pix": f"{output_root}/pix",
    }
    return JSONResponse(content=resp)
