# news_alert_app/backend/gdelt_client.py
"""
Cliente robusto para GDELT (ArtList CSV).
- Detecta dinámicamente columnas de URL/Title/Date/Language/Domain.
- Devuelve dicts normalizados con: date, title, url, domain, language.
"""

from __future__ import annotations
import csv
import io
import ssl
import urllib.parse
import urllib.request
from typing import List, Dict, Tuple
from urllib.parse import urlparse

BASE = "https://api.gdeltproject.org/api/v2/doc/doc"


def build_gdelt_url(
    query: str,
    maxrecords: int = 100,
    sort: str = "DateDesc",
    timespan: str = "1d",
) -> str:
    params = {
        "query": query,
        "mode": "ArtList",
        "maxrecords": str(maxrecords),
        "sort": sort,
        "format": "CSV",
        "timespan": timespan,
    }
    return f"{BASE}?{urllib.parse.urlencode(params)}"


def _pick_key(header_map: Dict[str, str], *candidates: str) -> str | None:
    for cand in candidates:
        c = cand.lower()
        if c in header_map:
            return header_map[c]
    return None


def fetch_csv(
    query: str,
    timeout: int = 12,
    maxrecords: int = 120,
    timespan: str = "12h",
    debug: bool = False,
) -> Tuple[List[Dict[str, str]], int]:
    """
    Descarga y parsea el CSV de GDELT.
    Retorna (rows, raw_len_bytes).
    """
    url = build_gdelt_url(query, maxrecords=maxrecords, timespan=timespan)
    ctx = ssl.create_default_context()

    with urllib.request.urlopen(url, timeout=timeout, context=ctx) as r:
        raw_bytes = r.read()
    raw = raw_bytes.decode("utf-8", errors="ignore")

    if debug:
        try:
            with open("last_gdelt.csv", "w", encoding="utf-8") as f:
                f.write(raw)
        except Exception:
            pass

    reader = csv.DictReader(io.StringIO(raw))
    rows: List[Dict[str, str]] = []

    header_map = {k.lower(): k for k in (reader.fieldnames or [])}

    url_key   = _pick_key(header_map, "URL", "SourceURL", "DocumentIdentifier", "Link")
    title_key = _pick_key(header_map, "Title", "DocumentTitle", "AltTitle")
    date_key  = _pick_key(header_map, "Date", "Timestamp", "SQLDate", "DateAdded", "DATE")
    lang_key  = _pick_key(header_map, "Language", "DocLanguage")
    dom_key   = _pick_key(header_map, "Domain")

    for row in reader:
        title  = (row.get(title_key, "") if title_key else "").strip()
        urlval = (row.get(url_key, "")   if url_key   else "").strip()
        date   = (row.get(date_key, "")  if date_key  else "").strip()
        lang   = (row.get(lang_key, "")  if lang_key  else "").strip().lower()
        domain = (row.get(dom_key, "")   if dom_key   else "").strip().lower()

        if not domain and urlval.startswith("http"):
            try:
                domain = urlparse(urlval).netloc.lower()
            except Exception:
                domain = ""

        if not title and not urlval:
            continue

        rows.append({
            "date": date,          # p.ej. '2025-09-26 19:50:22' o 'yyyymmddhhmmss'
            "title": title,
            "url": urlval,
            "domain": domain,      # puede venir vacío si no hay URL
            "language": lang,      # 'en', 'es', 'english', 'spanish'… (varía)
        })

    return rows, len(raw_bytes)
