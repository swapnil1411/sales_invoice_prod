#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Single-file GCS-only runner:
- Requires ROOT_PATH to be a gs:// URI (e.g., gs://bucket/prefix)
- Mirrors that prefix to a local temp dir
- Runs your existing process() logic locally (unchanged)
- Syncs the outputs back to the same GCS prefix
"""

import json, re, html, glob, sys, shutil, os, tempfile, subprocess, uuid
from pathlib import Path
from typing import Any, Dict, List, Tuple, Optional

# ===================== Your imports (unchanged) ===================== #
from transformer import map_mirakl_xml_to_template
# =================================================================== #

# ===================== env-config path (UNCHANGED) ================== #
CONFIG_TEMPLATE = "${ROOT_PATH}/config.json"
# =================================================================== #

# --------------------- helpers (UNCHANGED) --------------------- #
def expand_env_str(s: str) -> str:
    return os.path.expanduser(os.path.expandvars(s))

def expand_env_deep(obj: Any) -> Any:
    if isinstance(obj, dict):
        return {k: expand_env_deep(v) for k, v in obj.items()}
    if isinstance(obj, list):
        return [expand_env_deep(v) for v in obj]
    if isinstance(obj, str):
        return expand_env_str(obj)
    return obj

def safe_folder(name: str) -> str:
    s = (name or "").strip()
    s = re.sub(r"\s+", "_", s)
    s = re.sub(r"[^A-Za-z0-9_-]+", "", s)
    return s or "unknown"

def safe_filename(name: str) -> str:
    name = re.sub(r'[\x00-\x1f\x7f]', '', name)
    name = re.sub(r'[<>:"/\\|?*]+', ' ', name)
    name = re.sub(r'\s+', ' ', name).strip()
    return name or "untitled"

def as_list(v: Any) -> List[Any]:
    if v is None: return []
    return v if isinstance(v, list) else [v]

def normalize_payload(payload: Any) -> str:
    s = html.unescape(str(payload))
    return s.replace("\r\n", "\n").replace("\r", "\n")

def extract_invoice(src: dict) -> str:
    if str(src.get("AuditKey1", "")).strip() == "InvoiceNo" and src.get("AuditKeyValue1"):
        return str(src.get("AuditKeyValue1"))
    akv = as_list(src.get("AuditKeyValue"))
    inv = str(akv[0]) if akv else ""
    return inv

def record_matches(src: dict, want_desc_l: str, want_name_l: str) -> bool:
    descs_l = [str(x).strip().lower() for x in as_list(src.get("EventDescription"))]
    if want_desc_l not in descs_l:
        return False
    if want_name_l:
        names_l = [str(x).strip().lower() for x in as_list(src.get("EventName"))]
        return want_name_l in names_l
    return True

def make_unique(path: Path) -> Path:
    if not path.exists():
        return path
    stem, ext = path.stem, path.suffix
    i = 2
    while True:
        cand = path.with_name(f"{stem}_{i}{ext}")
        if not cand.exists():
            return cand
        i += 1

def norm_folder_key(folder: str) -> str:
    return (folder or "").strip().lower().replace(" ", "_")

# --------------------- naming rules (UNCHANGED) --------------------- #
NAMING_RULES: Dict[str, Tuple[str, str]] = {
    "producer-input": ("input",        "xml"),
    "mirakl-order":   ("mirakl_order", "json"),
    "mirakl-refund":  ("mirakl_refund","json"),
    "vertex":         ("vertex",       "txt"),
    "ip-us":          ("ip-us",        "txt"),
    "ip-uk":          ("ip-uk",        "txt"),
    "pix":            ("pix",          "xml"),
}

# --------------------- core (UNCHANGED) --------------------- #
def process(config_path: Path, input_date: Optional[str] = None) -> Dict[str, object]:
    raw = config_path.read_text(encoding="utf-8")
    cfg = json.loads(raw)
    cfg = expand_env_deep(cfg)

    base_dir = config_path.parent

    input_glob = cfg["input_glob"]
    if not Path(input_glob).is_absolute():
        input_glob = str((base_dir / input_glob).as_posix())

    out_root_cfg = cfg["output"]
    out_root = Path(out_root_cfg) if Path(out_root_cfg).is_absolute() else (base_dir / out_root_cfg)

    # keep: out_root.mkdir(parents=True, exist_ok=True)

    # Normalize the date folder once
    date_prefix = safe_folder(input_date) if input_date else None

    # Target root for THIS run (e.g., <output>/<date>)
    target_root = (out_root / date_prefix) if date_prefix else out_root

    if cfg.get("fresh", False) and target_root.exists():
        shutil.rmtree(target_root)

    # Normalize the date folder once
    date_prefix = safe_folder(input_date) if input_date else None

    raw_filters = cfg.get("filters", [])
    filters = []
    for f in raw_filters:
        folder_raw = f.get("folder", "") if "folder" in f else ""
        folder = safe_folder(folder_raw)
        want_desc = (f.get("event_description") or "").strip()
        want_name = (f.get("event_name") or "").strip()
        if not want_desc:
            continue

        folder_key = norm_folder_key(folder_raw or folder)
        prefix, ext = NAMING_RULES.get(folder_key, (folder_key or "output", "txt"))

        filters.append({
            "folder": folder,
            "folder_key": folder_key,
            "prefix": prefix,
            "ext": ext,
            "want_desc": want_desc,
            "want_desc_l": want_desc.lower(),
            "want_name": want_name,
            "want_name_l": want_name.lower()
        })

    per_folder_hits: Dict[str, int] = {flt["folder"]: 0 for flt in filters}
    stats = {"files_scanned": 0, "hits": 0, "written_files": []}

    for path_str in sorted(glob.glob(input_glob)):
        p = Path(path_str)
        if not p.is_file():
            continue
        stats["files_scanned"] += 1

        try:
            data = json.loads(p.read_text(encoding="utf-8", errors="ignore"))
        except Exception as e:
            print(f"[WARN] Could not parse {p.name}: {e}", file=sys.stderr)
            continue

        for resp in (data.get("responses") or []):
            for hit in ((resp or {}).get("hits", {}).get("hits") or []):
                src = (hit or {}).get("_source") or {}
                payloads = as_list(src.get("AuditAttachmentsData"))
                if not payloads:
                    continue

                for flt in filters:
                    if record_matches(src, flt["want_desc_l"], flt["want_name_l"]):
                        # -------- folder layout (UNCHANGED behavior intent) --------
                        base = (out_root / date_prefix) if date_prefix else out_root

                        if flt["folder_key"] == "producer-input":
                            folder_path = base / flt["folder"]
                        else:
                            leaf = "mirakl" if flt["folder_key"] in ("mirakl-order", "mirakl-refund") else flt["folder"]
                            folder_path = base / "expected-output" / leaf

                        folder_path.mkdir(parents=True, exist_ok=True)
                        # -----------------------------------------------------------

                        invoice = extract_invoice(src).strip()
                        invoice_sanitized = re.sub(r"[^A-Za-z0-9_-]+", "", invoice) or "unknown"

                        for pl in payloads:
                            filename = f"{flt['prefix']}_{invoice_sanitized}.{flt['ext']}"
                            out_path = folder_path / safe_filename(filename)
                            out_path = make_unique(out_path)

                            if flt["folder_key"] in ("mirakl-order", "mirakl-refund"):
                                try:
                                    mode = "order" if flt["folder_key"] == "mirakl-order" else "refund"
                                    mapped = map_mirakl_xml_to_template(str(pl), mode=mode)
                                    out_path.write_text(json.dumps(mapped, indent=2, ensure_ascii=False), encoding="utf-8")
                                except Exception as e:
                                    with out_path.open("w", encoding="utf-8") as f:
                                        f.write(f"# [WARN] mapping failed: {e}\n")
                                        f.write(str(pl))
                                        if not str(pl).endswith("\n"):
                                            f.write("\n")
                            else:
                                with out_path.open("w", encoding="utf-8") as f:
                                    f.write(str(pl))
                                    if not str(pl).endswith("\n"):
                                        f.write("\n")

                            stats["hits"] += 1
                            per_folder_hits[flt["folder"]] += 1
                            try:
                                stats["written_files"].append(str(out_path.relative_to(out_root)))
                            except Exception:
                                stats["written_files"].append(str(out_path))

    print("\nPer-folder matches:")
    for flt in filters:
        print(f"  - {flt['folder']}: {per_folder_hits.get(flt['folder'], 0)}")

    zeroes = [flt for flt in filters if per_folder_hits.get(flt["folder"], 0) == 0]
    if zeroes:
        print("\nNo matches for filters (EventDescription + EventName when provided):")
        for flt in zeroes:
            en = f" & EventName='{flt['want_name']}'" if flt["want_name"] else ""
            print(f"  - folder='{flt['folder']}', EventDescription='{flt['want_desc']}'{en}")

    base = out_root.resolve().parent
    folder_name = out_root.resolve().name
    date_prefix = safe_folder(date_prefix) if date_prefix else ""
    stats["paths"] = {
        "date": date_prefix or "",
        "root": str(base),
        "input": "{ROOT_PATH}" + "/" + folder_name + (("/" + date_prefix) if date_prefix else "") + "/" + "producer-input",
        "mirakl_output": "{ROOT_PATH}" + "/" + folder_name + (("/" + date_prefix) if date_prefix else "") + "/" + "expected-output/mirakl",
        "vertex_output": "{ROOT_PATH}" + "/" + folder_name + (("/" + date_prefix) if date_prefix else "") + "/" + "expected-output/vertex",
        "ip-us": "{ROOT_PATH}" + "/" + folder_name + (("/" + date_prefix) if date_prefix else "") + "/" + "expected-output/ip-us",
        "ip-uk": "{ROOT_PATH}" + "/" + folder_name + (("/" + date_prefix) if date_prefix else "") + "/" + "expected-output/ip-uk",
        "pix": "{ROOT_PATH}" + "/" + folder_name + (("/" + date_prefix) if date_prefix else "") + "/" + "expected-output/pix",
    }
    return stats

# =======================================================================
#                      G C S   H E L P E R S  (INLINE)
# =======================================================================
_GSUTIL_AVAILABLE: Optional[bool] = None

def _which(exe: str) -> Optional[str]:
    for p in os.environ.get("PATH", "").split(os.pathsep):
        cand = Path(p) / exe
        if cand.exists() and os.access(cand, os.X_OK):
            return str(cand)
    return None

def _gsutil_ok() -> bool:
    global _GSUTIL_AVAILABLE
    if _GSUTIL_AVAILABLE is not None:
        return _GSUTIL_AVAILABLE
    _GSUTIL_AVAILABLE = _which("gsutil") is not None
    return _GSUTIL_AVAILABLE

def _parse_gs_uri(uri: str) -> Tuple[str, str]:
    m = re.match(r"^gs://([^/]+)/?(.*)$", uri.strip())
    if not m:
        raise ValueError(f"ROOT_PATH must be a gs:// URI, got: {uri}")
    bucket, prefix = m.group(1), m.group(2)
    prefix = prefix.strip("/")
    return bucket, prefix

def _rsync_down(gs_uri: str, local_dir: Path) -> int:
    """
    Download *entire* prefix to local_dir using gsutil -m rsync -r.
    Returns number of downloaded objects (best-effort; we parse ls).
    """
    if not _gsutil_ok():
        raise RuntimeError("gsutil not found. Please install the Google Cloud SDK or add gsutil to PATH.")
    local_dir.mkdir(parents=True, exist_ok=True)

    # Count remote objects (best-effort)
    try:
        ls_out = subprocess.check_output(["gsutil", "ls", "-r", gs_uri], text=True, stderr=subprocess.STDOUT)
        objs = [line for line in ls_out.splitlines() if line.startswith("gs://") and not line.endswith("/")]
        count_before = len(objs)
    except Exception:
        count_before = 0

    # Sync down
    subprocess.check_call(["gsutil", "-m", "rsync", "-r", gs_uri, str(local_dir)])
    return count_before

def _rsync_up(local_dir: Path, gs_uri: str) -> int:
    """
    Upload *entire* local_dir to prefix using gsutil -m rsync -r.
    Returns number of local files uploaded (best-effort).
    """
    if not _gsutil_ok():
        raise RuntimeError("gsutil not found. Please install the Google Cloud SDK or add gsutil to PATH.")

    # Count local files (best-effort)
    count_local = sum(1 for _ in local_dir.rglob("*") if _.is_file())
    subprocess.check_call(["gsutil", "-m", "rsync", "-r", str(local_dir), gs_uri])
    return count_local

def run_with_gcs(config_path_str: str, process_fn, input_date: Optional[str]) -> Dict[str, Any]:
    """
    - Mirrors the entire ROOT_PATH prefix to a temp dir
    - Calls process_fn(local_config_path)
    - Pushes any changes (esp. outputs) back to the same prefix
    - Returns stats + GCS transfer info
    """
    root_uri = os.environ.get("ROOT_PATH")
    if not root_uri:
        raise RuntimeError("ROOT_PATH not set. Export ROOT_PATH to your gs:// prefix.")
    if not root_uri.startswith("gs://"):
        raise RuntimeError(f"ROOT_PATH must be a gs:// URI, got: {root_uri}")

    # Make a unique temp workspace
    tmp_root = Path(tempfile.gettempdir()) / f"gcs_mirror_{uuid.uuid4().hex}"
    tmp_root.mkdir(parents=True, exist_ok=True)

    # Mirror down
    print(f"[INFO] Mirroring down from {root_uri} -> {tmp_root}")
    downloaded = _rsync_down(root_uri, tmp_root)

    # Resolve local config path (always <tmp_root>/config.json)
    local_cfg = (tmp_root / "config.json").resolve()
    if not local_cfg.exists():
        raise FileNotFoundError(f"config.json not found in {root_uri}. Expected at {root_uri}/config.json")

    # Run your unchanged logic
    stats = process_fn(local_cfg)

    # Push up everything (so outputs under cfg['output'] get synced)
    print(f"[INFO] Syncing outputs back to {root_uri} from {tmp_root}")
    uploaded = _rsync_up(tmp_root, root_uri)

    # Annotate stats
    stats["gcs_downloaded"] = downloaded
    stats["gcs_uploaded"] = uploaded
    stats["gcs_tmp_root"] = str(tmp_root)

    # NOTE: We do NOT delete tmp_root so you can inspect if needed. Uncomment to clean up:
    # shutil.rmtree(tmp_root, ignore_errors=True)

    return stats

# =======================================================================
#                                M A I N
# =======================================================================
def main(input_date: Optional[str] = None):
    if input_date:
        print(f"[INFO] Using date prefix: {input_date}")

    # Require ROOT_PATH to be gs://... (GCS-only mode)
    root = os.environ.get("ROOT_PATH", "").strip()
    if not root or not root.startswith("gs://"):
        print("[ERROR] GCS-only mode: export ROOT_PATH to a gs:// prefix.", file=sys.stderr)
        print("Example:", file=sys.stderr)
        print('  export ROOT_PATH="gs://qa-automation-test_x35-dev/sales_invoice_kibana_logs/my-test-2025-09-05"', file=sys.stderr)
        sys.exit(1)

    # config path is always ROOT_PATH/config.json, but we pass a string for parity
    cfg_path_str = expand_env_str(CONFIG_TEMPLATE)

    try:
        stats = run_with_gcs(
            config_path_str=cfg_path_str,
            process_fn=lambda local_cfg_path: process(local_cfg_path, input_date=input_date),
            input_date=input_date,
        )
    except subprocess.CalledProcessError as e:
        print(f"[ERROR] gsutil command failed: {e}", file=sys.stderr)
        sys.exit(2)
    except Exception as e:
        print(f"[ERROR] {e}", file=sys.stderr)
        sys.exit(3)

    print(f"\nScanned: {stats.get('files_scanned', 0)} JSON files")
    print(f"Total records written: {stats.get('hits', 0)}")
    print(f"GCS downloaded: {stats.get('gcs_downloaded', 0)}, uploaded: {stats.get('gcs_uploaded', 0)}")
    print(f"Temp mirror: {stats.get('gcs_tmp_root', '-')}")

if __name__ == "__main__":
    arg_date = sys.argv[1] if len(sys.argv) > 1 else None
    main(arg_date)
