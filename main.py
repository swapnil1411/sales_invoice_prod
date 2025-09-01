#!/usr/bin/env python3
import json, re, html, glob, sys, shutil, os
from pathlib import Path
from typing import Any, Dict, List, Tuple, Optional

# Transformer (unchanged)
from transformer import map_mirakl_xml_to_template

# Try to import the GCS bridge. If missing, we’ll run locally.
try:
    from gcs_utils import run_with_optional_gcs  # <-- your new helper file
except Exception:
    run_with_optional_gcs = None

# ===================== env-config path (as requested) ===================== #
CONFIG_TEMPLATE = "${ROOT_PATH}/config.json"
# ========================================================================== #

# --------------------- helpers ---------------------

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

# --------------------- naming rules ---------------------

NAMING_RULES: Dict[str, Tuple[str, str]] = {
    "producer-input":         ("input",        "xml"),
    "mirakl-order":  ("mirakl_order", "json"),
    "mirakl-refund": ("mirakl_refund","json"),
    "vertex":        ("vertex",       "txt"),
    "ip-us":         ("ip-us",        "txt"),
    "ip-uk":         ("ip-uk",        "txt"),
    "pix":           ("pix",          "xml"),
}

# --------------------- core ---------------------

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
                        # -------- NEW: build folder_path (producer-input unchanged; others under expected-output) --------
                        base = (out_root / date_prefix) if date_prefix else out_root
                
                        if flt["folder_key"] == "producer-input":
                            # keep original behavior
                            folder_path = base / flt["folder"]
                        else:
                            # add the 'expected-output' subfolder for everything else
                            # (optional: collapse mirakl-order/refund into a single 'mirakl' folder)
                            leaf = "mirakl" if flt["folder_key"] in ("mirakl-order", "mirakl-refund") else flt["folder"]
                            folder_path = base / "expected-output" / leaf
                
                        folder_path.mkdir(parents=True, exist_ok=True)
                        # -----------------------------------------------------------------------------------------------
                
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
    stats["paths"] = {
        "date": date_prefix or "",
        "root": str(base),
        "input": "{ROOT_PATH}" + "/" + folder_name +"/"+ date_prefix + "/" + "producer-input",
        "mirakl_output": "{ROOT_PATH}" + "/" + folder_name + "/"+ date_prefix + "/" + "expected-output/mirakl",
        "vertex_output": "{ROOT_PATH}"  + "/" + folder_name +"/"+ date_prefix + "/" +"expected-output/vertex",
        "ip-us": "{ROOT_PATH}"  + "/" + folder_name +"/"+ date_prefix + "/" +"expected-output/ip-us",
        "ip-uk": "{ROOT_PATH}"  + "/" + folder_name +"/"+ date_prefix + "/" +"expected-output/ip-uk",
        "pix": "{ROOT_PATH}"  + "/" + folder_name +"/"+ date_prefix + "/" +"expected-output/pix",
    }
    return stats

def main(input_date: Optional[str] = None):
    # show the date we’re using (helps when called from API)
    if input_date:
        print(f"[INFO] Using date prefix: {input_date}")

    cfg_path_str = expand_env_str(CONFIG_TEMPLATE)
    if "${ROOT_PATH}" in CONFIG_TEMPLATE or "$ROOT_PATH" in CONFIG_TEMPLATE:
        root = os.environ.get("ROOT_PATH", ".")
        cfg_path_str = str(Path(root) / "config.json")

    # If the GCS bridge is available, let it decide (gs:// vs local) and
    # run our existing process() on a local mirror when needed.
    if run_with_optional_gcs:
        try:
            stats = run_with_optional_gcs(
                config_path_str=cfg_path_str,
                process_fn=lambda local_cfg_path: process(local_cfg_path, input_date=input_date),
            )
        except Exception as e:
            print(f"[WARN] GCS run failed ({e}); falling back to local.")
            cfg_path = Path(cfg_path_str).resolve()
            if not cfg_path.exists():
                print(f"[ERROR] Config not found at: {cfg_path}", file=sys.stderr)
                sys.exit(1)
            stats = process(cfg_path, input_date=input_date)
    else:
        # Local-only fallback
        print("[INFO] gcs_utils not found; running locally.")
        cfg_path = Path(cfg_path_str).resolve()
        if not cfg_path.exists():
            print(f"[ERROR] Config not found at: {cfg_path}", file=sys.stderr)
            sys.exit(1)
        stats = process(cfg_path, input_date=input_date)

    print(f"\nScanned: {stats.get('files_scanned', 0)} JSON files")
    print(f"Total records written: {stats.get('hits', 0)}")
    if "gcs_downloaded" in stats or "gcs_uploaded" in stats:
        print(f"GCS downloaded: {stats.get('gcs_downloaded', 0)}, uploaded: {stats.get('gcs_uploaded', 0)}")
        print(f"Temp mirror: {stats.get('gcs_tmp_root', '-')}")

if __name__ == "__main__":
    # Optional CLI support: python main.py 2025-08-25
    arg_date = sys.argv[1] if len(sys.argv) > 1 else None
    main(arg_date)
