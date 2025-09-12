#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
gcs_util.py
A tiny bridge that lets your existing local pipeline run against Google Cloud Storage.

Usage in main.py:
    from gcs_util import run_with_optional_gcs
    ...
    stats = run_with_optional_gcs(
        config_path_str=cfg_path_str,
        process_fn=lambda local_cfg_path: process(local_cfg_path, input_date=input_date)
    )

What it does:
- If config.json OR cfg["input_glob"]/cfg["output"] use gs://, we:
  1) Download config.json (if gs://)
  2) Expand envs; if input_glob is gs:// pattern, list blobs and download to a local temp "mirror"
  3) Rewrite cfg with local paths; write a local config.json
  4) Call your existing process() with that local config.json
  5) Upload the produced local output folder to gs:// output (and delete old content if fresh=true)
- Otherwise, we just call process_fn(Path(config_path_str)) and return its result.

Auth:
- The Google SDK respects standard auth (ADC).
  Export GOOGLE_APPLICATION_CREDENTIALS or use gcloud auth application-default login.

Dependency:
    pip install google-cloud-storage
"""
from __future__ import annotations

import json, os, re, shutil, tempfile
from pathlib import Path
from typing import Callable, Dict, List, Optional, Tuple
from fnmatch import fnmatch

# ---------------- Path helpers ----------------

_GS_RE = re.compile(r"^gs://([^/]+)/?(.*)$")

def is_gs_uri(s: str) -> bool:
    return bool(_GS_RE.match(s or ""))

def split_gs_uri(gs_uri: str) -> Tuple[str, str]:
    """
    Returns (bucket, key) where key may be ''.
    """
    m = _GS_RE.match(gs_uri)
    if not m:
        raise ValueError(f"Not a gs:// URI: {gs_uri}")
    return m.group(1), m.group(2)

def _prefix_before_wildcard(path: str) -> str:
    """
    Return the longest static prefix before any wildcard *, ?, [ in the path.
    Used to reduce list_blobs scope.
    """
    specials = [path.find("*"), path.find("?"), path.find("[")]
    specials = [i for i in specials if i != -1]
    if not specials:
        return path
    cut = min(specials)
    slash = path.rfind("/", 0, cut)
    return path[: slash + 1] if slash >= 0 else ""

# ---------------- Core GCS client (lazy import) ----------------

def _get_gcs_client():
    from google.cloud import storage  # lazy import
    return storage.Client()

# ---------------- Download / Upload ----------------

def gcs_list_blobs_matching(gs_pattern: str) -> List[str]:
    """
    List object URIs matching a gs:// pattern with wildcards in the key-part.
    """
    bucket_name, key_pattern = split_gs_uri(gs_pattern)
    client = _get_gcs_client()
    bucket = client.bucket(bucket_name)

    # List with a static prefix, then fnmatch against the whole key
    prefix = _prefix_before_wildcard(key_pattern)
    uris: List[str] = []
    for blob in client.list_blobs(bucket, prefix=prefix):
        key = blob.name
        if fnmatch(key, key_pattern):
            uris.append(f"gs://{bucket_name}/{key}")
    return uris

def gcs_read_text(gs_uri: str, encoding: str = "utf-8") -> str:
    bucket_name, key = split_gs_uri(gs_uri)
    client = _get_gcs_client()
    bucket = client.bucket(bucket_name)
    blob = bucket.blob(key)
    return blob.download_as_text(encoding=encoding)

def gcs_write_text(gs_uri: str, content: str, encoding: str = "utf-8") -> None:
    bucket_name, key = split_gs_uri(gs_uri)
    client = _get_gcs_client()
    bucket = client.bucket(bucket_name)
    blob = bucket.blob(key)
    blob.upload_from_string(content.encode(encoding), content_type="application/json")

def gcs_delete_prefix(gs_uri_prefix: str) -> int:
    """
    Delete all objects under the given gs:// prefix. Returns count deleted.
    """
    bucket_name, key_prefix = split_gs_uri(gs_uri_prefix)
    client = _get_gcs_client()
    bucket = client.bucket(bucket_name)
    cnt = 0
    for blob in client.list_blobs(bucket, prefix=key_prefix.rstrip("/") + "/"):
        blob.delete()
        cnt += 1
    return cnt

def gcs_upload_dir(local_dir: Path, gs_prefix: str) -> int:
    """
    Recursively upload a local directory to a gs:// prefix.
    Preserves the directory structure under local_dir.
    Returns number of files uploaded.
    """
    bucket_name, key_prefix = split_gs_uri(gs_prefix)
    client = _get_gcs_client()
    bucket = client.bucket(bucket_name)
    key_prefix = key_prefix.rstrip("/")

    uploaded = 0
    for root, _, files in os.walk(local_dir):
        root_path = Path(root)
        for fname in files:
            lpath = root_path / fname
            rel = lpath.relative_to(local_dir).as_posix()
            gcs_key = f"{key_prefix}/{rel}" if key_prefix else rel
            blob = bucket.blob(gcs_key)
            # Try to infer content type by extension
            if lpath.suffix.lower() == ".json":
                blob.upload_from_filename(str(lpath), content_type="application/json")
            elif lpath.suffix.lower() in (".xml", ".txt", ".log"):
                blob.upload_from_filename(str(lpath), content_type="text/plain")
            else:
                blob.upload_from_filename(str(lpath))
            uploaded += 1
    return uploaded

def gcs_download_to_dir(gs_uris: List[str], local_dir: Path) -> List[Path]:
    """
    Download a list of gs:// object URIs into local_dir, preserving the suffix
    path under the 'static prefix' of the pattern. If URIs are fully-qualified,
    we just mirror key paths.
    """
    local_paths: List[Path] = []
    client = _get_gcs_client()
    local_dir.mkdir(parents=True, exist_ok=True)

    # Group by bucket to reduce client calls
    by_bucket: Dict[str, List[str]] = {}
    for u in gs_uris:
        b, k = split_gs_uri(u)
        by_bucket.setdefault(b, []).append(k)

    for bucket_name, keys in by_bucket.items():
        bucket = client.bucket(bucket_name)
        for key in keys:
            tgt = local_dir / key  # mirror object key under local_dir
            tgt.parent.mkdir(parents=True, exist_ok=True)
            blob = bucket.blob(key)
            blob.download_to_filename(str(tgt))
            local_paths.append(tgt)
    return local_paths

# ---------------- Runner ----------------

def run_with_optional_gcs(
    config_path_str: str,
    process_fn: Callable[[Path], Dict[str, object]],
    tmp_root: Optional[Path] = None,
) -> Dict[str, object]:
    """
    If config_path/input_glob/output use gs://, mirror remote to local, run process_fn, re-upload.
    Otherwise, just call process_fn(Path(config_path_str)).

    Returns whatever process_fn returns, augmented with 'gcs_downloaded'/'gcs_uploaded' counters when applicable.
    """
    # Simple local path case fast-path
    might_be_remote_cfg = is_gs_uri(config_path_str)
    print(f"[GCS] fil😂e Config path: {config_path_str} (remote={might_be_remote_cfg})")
    # If config.json is local, we still may have gs:// in its values
    if not might_be_remote_cfg and not Path(config_path_str).exists():
        raise FileNotFoundError(f"Config not found at: {config_path_str}")

    if not might_be_remote_cfg:
        # Peek the local config to check for remote IO within it
        cfg = json.loads(Path(config_path_str).read_text("utf-8"))
        input_glob = cfg.get("input_glob", "")
        output = cfg.get("output", "")
        if not (is_gs_uri(input_glob) or is_gs_uri(output)):
            # All local — run as-is
            return process_fn(Path(config_path_str))

    # Remote case (either config.json is gs:// or fields inside point to gs://)
    tmp_root = tmp_root or Path(tempfile.mkdtemp(prefix="gcs_mirror_"))
    tmp_cfg_dir = tmp_root / "cfg"
    tmp_in_dir  = tmp_root / "inputs"
    tmp_out_dir = tmp_root / "outputs"
    tmp_cfg_dir.mkdir(parents=True, exist_ok=True)
    tmp_in_dir.mkdir(parents=True, exist_ok=True)
    tmp_out_dir.mkdir(parents=True, exist_ok=True)

    # 1) Load config.json (remote or local)
    if might_be_remote_cfg:
        cfg_text = gcs_read_text(config_path_str)
        cfg = json.loads(cfg_text)
    else:
        cfg = json.loads(Path(config_path_str).read_text("utf-8"))

    # 2) Resolve remote inputs
    input_glob = cfg.get("input_glob", "")
    output = cfg.get("output", "")
    fresh_flag = bool(cfg.get("fresh", False))

    downloaded = 0
    if is_gs_uri(input_glob):
        uris = gcs_list_blobs_matching(input_glob)
        gcs_download_to_dir(uris, tmp_in_dir)  # mirrors keys under tmp_in_dir
        downloaded = len(uris)
        # rewrite input_glob to local temp mirror
        # Find static prefix in the key pattern and point glob to that mirror
        _, key_pattern = split_gs_uri(input_glob)
        local_glob = (tmp_in_dir / key_pattern).as_posix()
        cfg["input_glob"] = local_glob
    else:
        # Local input_glob; copy as-is (no changes)
        pass

    # 3) Rewrite output path to local temp
    if is_gs_uri(output):
        cfg["_remote_output"] = output  # stash for upload step
        cfg["output"] = str(tmp_out_dir)

        # If fresh requested, proactively clear remote output prefix
        if fresh_flag:
            try:
                deleted = gcs_delete_prefix(output)
                print(f"[GCS] Cleared remote output prefix ({deleted} objects): {output}")
            except Exception as e:
                print(f"[GCS][WARN] Failed to clear remote prefix {output}: {e}")
    else:
        cfg["_remote_output"] = ""

    # 4) Write the modified config locally and run your existing pipeline
    local_cfg_path = tmp_cfg_dir / "config.json"
    local_cfg_path.write_text(json.dumps(cfg, indent=2), encoding="utf-8")
    stats = process_fn(local_cfg_path)

    # 5) If output was remote, upload local output dir
    uploaded = 0
    remote_out = cfg.get("_remote_output") or ""
    if remote_out:
        uploaded = gcs_upload_dir(tmp_out_dir, remote_out)

    # 6) Return stats + GCS extras
    stats = dict(stats or {})
    stats["gcs_downloaded"] = downloaded
    stats["gcs_uploaded"] = uploaded
    stats["gcs_tmp_root"] = str(tmp_root)
    return stats
