import hashlib
import json
import time
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

import requests


def sha256_text(text: str) -> str:
    return hashlib.sha256(text.encode("utf-8")).hexdigest()


def iso_from_unix(ts: int) -> str:
    return datetime.fromtimestamp(ts, tz=timezone.utc).isoformat()


def normalize_text(text: str) -> str:
    return (text or "").strip()


def make_content_hash(title: str, body: str, url: str, posted_at: Optional[str]) -> str:
    payload = json.dumps(
        {"title": title or "", "body": body or "",
            "url": url or "", "posted_at": posted_at or ""},
        ensure_ascii=False,
        sort_keys=True,
    )
    return sha256_text(payload)


def make_content_hash_with_updated(
    title: str,
    body: str,
    url: str,
    posted_at: Optional[str],
    updated_at: Optional[str],
) -> str:
    payload = json.dumps(
        {
            "title": title or "",
            "body": body or "",
            "url": url or "",
            "posted_at": posted_at or "",
            "updated_at": updated_at or "",
        },
        sort_keys=True,
    )
    return sha256_text(payload)


def keyword_hit(text: str, keywords: List[str]) -> bool:
    if not keywords:
        return True
    haystack = (text or "").lower()
    return any(keyword.lower() in haystack for keyword in keywords)


def http_get_json(
    url: str,
    headers: Optional[Dict[str, str]] = None,
    params: Optional[Dict[str, Any]] = None,
    timeout_s: int = 20,
) -> Any:
    resp = requests.get(url, headers=headers or {},
                        params=params or {}, timeout=timeout_s)
    # Basic 429 handling (simple)
    if resp.status_code == 429:
        retry_after = resp.headers.get("retry-after")
        wait_s = int(
            retry_after) if retry_after and retry_after.isdigit() else 5
        time.sleep(wait_s)
        resp = requests.get(url, headers=headers or {},
                            params=params or {}, timeout=timeout_s)
    resp.raise_for_status()
    return resp.json()


def strip_hn_html(text: str) -> str:
    # Very simple HTML stripping for HN "text" field
    if not text:
        return ""
    s = text.replace("<p>", "\n").replace("<br>", "\n").replace(
        "<br/>", "\n").replace("<br />", "\n")
    # Remove tags
    out = []
    in_tag = False
    for ch in s:
        if ch == "<":
            in_tag = True
            continue
        if ch == ">":
            in_tag = False
            continue
        if not in_tag:
            out.append(ch)
    s = "".join(out)
    # Decode common entities
    return (
        s.replace("&quot;", '"')
        .replace("&#x27;", "'")
        .replace("&amp;", "&")
        .replace("&lt;", "<")
        .replace("&gt;", ">")
        .strip()
    )
