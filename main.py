#!/usr/bin/env python3
import json, re, html, glob, sys, shutil, os
from pathlib import Path
from typing import Any, Dict, List, Tuple

# ===================== env-config path (as requested) ===================== #
# Use config path like this (defined in code), then expand envs:
CONFIG_TEMPLATE = "${ROOT_PATH}/config.json"
# ========================================================================== #

# --------------------- helpers ---------------------

def expand_env_str(s: str) -> str:
    """Expand $VAR / ${VAR} and ~ in a single string."""
    return os.path.expanduser(os.path.expandvars(s))

def expand_env_deep(obj: Any) -> Any:
    """
    Recursively expand env vars in dict/list/str.
    Leaves non-strings unchanged.
    """
    if isinstance(obj, dict):
        return {k: expand_env_deep(v) for k, v in obj.items()}
    if isinstance(obj, list):
        return [expand_env_deep(v) for v in obj]
    if isinstance(obj, str):
        return expand_env_str(obj)
    return obj

def safe_folder(name: str) -> str:
    """
    Preserve case; allow letters, numbers, underscores, hyphens.
    Convert whitespace to underscores; strip other chars.
    """
    s = (name or "").strip()
    s = re.sub(r"\s+", "_", s)
    s = re.sub(r"[^A-Za-z0-9_-]+", "", s)
    return s or "unknown"

def safe_filename(name: str) -> str:
    """Human-readable filenames; remove/normalize unsafe characters."""
    name = re.sub(r'[\x00-\x1f\x7f]', '', name)       # control chars
    name = re.sub(r'[<>:"/\\|?*]+', ' ', name)        # forbidden on Windows/macOS
    name = re.sub(r'\s+', ' ', name).strip()
    return name or "untitled"

def as_list(v: Any) -> List[Any]:
    if v is None: return []
    return v if isinstance(v, list) else [v]

def normalize_payload(payload: Any) -> str:
    s = html.unescape(str(payload))
    return s.replace("\r\n", "\n").replace("\r", "\n")

def extract_invoice(src: dict) -> str:
    # Preferred: explicit InvoiceNo pair
    if str(src.get("AuditKey1", "")).strip() == "InvoiceNo" and src.get("AuditKeyValue1"):
        return str(src.get("AuditKeyValue1"))
    # Fallback: first element in AuditKeyValue
    akv = as_list(src.get("AuditKeyValue"))
    inv = str(akv[0]) if akv else ""
    return inv

def record_matches(src: dict, want_desc_l: str, want_name_l: str) -> bool:
    """
    Match logic:
      - EventDescription must match (case-insensitive exact).
      - If want_name_l is non-empty, EventName must also match.
    """
    descs_l = [str(x).strip().lower() for x in as_list(src.get("EventDescription"))]
    if want_desc_l not in descs_l:
        return False
    if want_name_l:
        names_l = [str(x).strip().lower() for x in as_list(src.get("EventName"))]
        return want_name_l in names_l
    return True

def make_unique(path: Path) -> Path:
    """If 'path' exists, append _2, _3, ... before extension until unique."""
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

# Folder-name (normalized) -> (file prefix, extension)
NAMING_RULES: Dict[str, Tuple[str, str]] = {
    "input":         ("input",        "xml"),
    "mirakl-order":  ("mirakl_order", "json"),
    "mirakl-refund": ("mirakl_refund","json"),
    "vertex":        ("vertex",       "txt"),
    "ip-us":         ("ip-us",        "txt"),
    "ip-uk":         ("ip-uk",        "txt"),
    "pix":           ("pix",          "xml"),
}

# --------------------- core ---------------------

def process(config_path: Path) -> Dict[str, object]:
    raw = config_path.read_text(encoding="utf-8")
    cfg = json.loads(raw)

    # Expand env vars & ~ throughout the config (so ${ROOT_PATH} inside values works)
    cfg = expand_env_deep(cfg)

    base_dir = config_path.parent

    # Resolve input glob relative to config.json (after expansion)
    input_glob = cfg["input_glob"]
    if not Path(input_glob).is_absolute():
        input_glob = str((base_dir / input_glob).as_posix())

    # Resolve output root (after expansion)
    out_root_cfg = cfg["output"]
    out_root = Path(out_root_cfg) if Path(out_root_cfg).is_absolute() else (base_dir / out_root_cfg)

    # Fresh: clear output
    if cfg.get("fresh", False) and out_root.exists():
        shutil.rmtree(out_root)
    out_root.mkdir(parents=True, exist_ok=True)

    # Compile filters
    raw_filters = cfg.get("filters", [])
    filters = []
    for f in raw_filters:
        folder_raw = f.get("folder", "") if "folder" in f else ""
        folder = safe_folder(folder_raw)
        want_desc = (f.get("event_description") or "").strip()
        want_name = (f.get("event_name") or "").strip()
        if not want_desc:
            # skip invalid filter
            continue

        folder_key = norm_folder_key(folder_raw or folder)
        prefix, ext = NAMING_RULES.get(folder_key, (folder_key or "output", "txt"))

        filters.append({
            "folder": folder,                 # pretty folder (preserve case/hyphen)
            "folder_key": folder_key,         # normalized for rules lookup
            "prefix": prefix,                 # file prefix
            "ext": ext,                       # file extension (without dot)
            "want_desc": want_desc,
            "want_desc_l": want_desc.lower(),
            "want_name": want_name,
            "want_name_l": want_name.lower()
        })

    per_folder_hits: Dict[str, int] = {flt["folder"]: 0 for flt in filters}
    stats = {"files_scanned": 0, "hits": 0, "written_files": []}

    # Scan files
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
                        folder_path = out_root / flt["folder"]
                        folder_path.mkdir(parents=True, exist_ok=True)

                        invoice = extract_invoice(src).strip()
                        invoice_sanitized = re.sub(r"[^A-Za-z0-9_-]+", "", invoice) or "unknown"

                        for pl in payloads:
                            filename = f"{flt['prefix']}_{invoice_sanitized}.{flt['ext']}"
                            out_path = folder_path / safe_filename(filename)
                            out_path = make_unique(out_path)  # avoid collisions
                            with out_path.open("w", encoding="utf-8") as f:
                                f.write(f"# ----- source={p.name} invoice={invoice} -----\n")
                                f.write(normalize_payload(pl))
                                if not str(pl).endswith("\n"):
                                    f.write("\n")
                            stats["hits"] += 1
                            per_folder_hits[flt["folder"]] += 1
                            stats["written_files"].append(str(out_path.relative_to(out_root)))

    # Summary
    print("\nPer-folder matches:")
    for flt in filters:
        print(f"  - {flt['folder']}: {per_folder_hits.get(flt['folder'], 0)}")

    zeroes = [flt for flt in filters if per_folder_hits.get(flt["folder"], 0) == 0]
    if zeroes:
        print("\nNo matches for filters (EventDescription + EventName when provided):")
        for flt in zeroes:
            en = f" & EventName='{flt['want_name']}'" if flt["want_name"] else ""
            print(f"  - folder='{flt['folder']}', EventDescription='{flt['want_desc']}'{en}")

    return stats

def main():
    # Build config path from env-defined template, not CLI args
    cfg_path_str = expand_env_str(CONFIG_TEMPLATE)

    # If ROOT_PATH wasn't set, fall back to using current dir
    if "${ROOT_PATH}" in CONFIG_TEMPLATE or "$ROOT_PATH" in CONFIG_TEMPLATE:
        # Template still contains unresolved var; use ROOT_PATH if present else "."
        root = os.environ.get("ROOT_PATH", ".")
        cfg_path_str = str(Path(root) / "config.json")

    cfg_path = Path(cfg_path_str).resolve()
    if not cfg_path.exists():
        print(f"[ERROR] Config not found at: {cfg_path}", file=sys.stderr)
        sys.exit(1)

    stats = process(cfg_path)
    print(f"\nScanned: {stats['files_scanned']} JSON files")
    print(f"Total records written: {stats['hits']}")

if __name__ == "__main__":
    main()
