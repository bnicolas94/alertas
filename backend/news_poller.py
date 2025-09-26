# news_alert_app/backend/news_poller.py
"""
Poller de noticias con GDELT (y fallback opcional a Reuters).
- Encola TODO y adjunta: domain, language, category, tickers.
- Detección de idioma en backend con langdetect (si está disponible).
"""

from __future__ import annotations
import asyncio
import datetime
import re
import urllib.request
import xml.etree.ElementTree as ET
from email.utils import parsedate_to_datetime
from typing import List, Dict
from urllib.parse import urlparse

from .gdelt_client import fetch_csv

# ---------- Detección de idioma (opcional) ----------
try:
    from langdetect import detect, DetectorFactory
    DetectorFactory.seed = 0  # determinismo
    def safe_detect_lang(text: str) -> str:
        try:
            if not text or not text.strip():
                return "unk"
            code = detect(text)
            return (code or "unk").lower()
        except Exception:
            return "unk"
except Exception:
    # Si no está instalado langdetect, seguimos sin romper
    def safe_detect_lang(text: str) -> str:
        return "unk"

# ==========================
# Config
# ==========================
GDELT_QUERY = (
    '(stocks OR stock OR shares OR market OR earnings OR EPS OR revenue OR acquisition OR merger '
    'OR resigns OR resignation OR contract OR lithium OR oil OR mining OR semiconductor OR government)'
)
POLL_INTERVAL_SEC = 30
BATCH_MAX = 120
TIMESPAN = "12h"

# Fallback RSS (si alguna vez querés activarlo)
REUTERS_RSS = "https://feeds.reuters.com/reuters/businessNews"

STOP_UPPER = {
    "THE","AND","FOR","WITH","FROM","THIS","THAT","WAS","WILL","HAVE","HAS",
    "USA","US","CEO","CFO","DOE","DOD","IPO","ETF","FDA","SEC","EU","UK",
    "LITHIUM","OIL","GAS","BANK","NEWS","MERGER","ACQUISITION","Q1","Q2","Q3","Q4"
}

RE_GOV_STAKE = re.compile(r"\b(government|state)\b.*\b(stake|equity|share)\b", re.I)
RE_CEO_RESIGN = re.compile(r"\b(CEO|CFO)\b.*\b(resigns?|steps down|resignation)\b", re.I)
RE_MA         = re.compile(r"\b(acquisition|acquire|acquired|merger|merging|combine)\b", re.I)
RE_EARNINGS   = re.compile(r"\b(earnings|guidance|EPS|revenue)\b", re.I)
RE_CONTRACT   = re.compile(r"\b(contract|award|offtake|MoU)\b", re.I)
UPPER_TOKEN   = re.compile(r"\b[A-Z]{2,5}\b")


def iso_now_utc() -> str:
    return datetime.datetime.utcnow().replace(microsecond=0).isoformat() + "Z"


def to_iso_utc(date_str: str) -> str:
    if not date_str:
        return iso_now_utc()
    # 1) GDELT clásico yyyymmddhhmmss
    try:
        dt = datetime.datetime.strptime(date_str, "%Y%m%d%H%M%S")
        return dt.isoformat() + "Z"
    except Exception:
        pass
    # 2) 'YYYY-MM-DD HH:MM:SS'
    try:
        dt = datetime.datetime.fromisoformat(date_str.replace(" ", "T"))
        if dt.tzinfo is None:
            return dt.isoformat() + "Z"
        return dt.astimezone(datetime.timezone.utc).isoformat()
    except Exception:
        pass
    # 3) RSS (RFC-2822)
    try:
        dt = parsedate_to_datetime(date_str)
        return dt.astimezone(datetime.timezone.utc).isoformat()
    except Exception:
        return iso_now_utc()


def classify(title: str) -> str:
    t = title or ""
    if RE_GOV_STAKE.search(t): return "GovStake"
    if RE_CEO_RESIGN.search(t): return "CEOResignation"
    if RE_MA.search(t):         return "M&A"
    if RE_EARNINGS.search(t):   return "Earnings"
    if RE_CONTRACT.search(t):   return "Contract"
    return "News"


def guess_tickers(title: str) -> List[str]:
    seen, out = set(), []
    for tok in UPPER_TOKEN.findall((title or "")):
        if tok in STOP_UPPER:
            continue
        if tok not in seen and 2 <= len(tok) <= 5:
            seen.add(tok); out.append(tok)
    return out[:6]


def fetch_reuters_rss(limit: int = 50) -> List[Dict[str, str]]:
    try:
        with urllib.request.urlopen(REUTERS_RSS, timeout=10) as r:
            xml = r.read().decode("utf-8", errors="ignore")
        root = ET.fromstring(xml)
        items = root.findall(".//item")
        out: List[Dict[str, str]] = []
        for it in items[:limit]:
            title = (it.findtext("title") or "").strip()
            link  = (it.findtext("link") or "").strip()
            pub   = (it.findtext("{http://purl.org/dc/elements/1.1/}date")
                     or it.findtext("pubDate") or "").strip()
            dom = ""
            try: dom = urlparse(link).netloc.lower()
            except Exception: pass
            out.append({
                "date": pub,
                "domain": dom or "reuters.com",
                "language": "en",
                "title": title or "(sin título)",
                "url": link,
                "ts": pub if pub else iso_now_utc(),
            })
        return out
    except Exception as e:
        print("[RSS] error:", repr(e))
        return []


def normalize_lang(lang: str) -> str:
    """
    Devuelve 'es', 'en' u otro código (ej. 'ru', 'el', 'ta', 'unk').
    Acepta valores como 'spanish', 'english', 'es-ES', etc.
    """
    if not lang:
        return "unk"
    l = lang.strip().lower()
    if l.startswith("es") or l == "spanish": return "es"
    if l.startswith("en") or l == "english": return "en"
    return l  # ru, el, ta, etc., o 'unk'


async def poll_news(queue: asyncio.Queue):
    """
    Encola TODO lo que llega y agrega 'domain' + 'language' (detectado).
    Dedup por URL o (titulo|fecha) si no hay URL.
    """
    seen: set[str] = set()

    # Seed inicial
    await queue.put({
        "headline": "✅ Fuente conectada (seed de prueba)",
        "summary": "WS y UI OK — filtros en el FRONT",
        "tickers": ["TEST"],
        "category": "Info",
        "url": "",
        "ts": iso_now_utc(),
        "domain": "",
        "language": "es",
    })
    print("[SEED] enviado")

    backoff = POLL_INTERVAL_SEC

    while True:
        try:
            # 1) GDELT
            rows, raw_len = fetch_csv(
                GDELT_QUERY,
                timeout=12,
                maxrecords=BATCH_MAX,
                timespan=TIMESPAN,
                debug=False,
            )
            print(f"[GDELT] raw_bytes={raw_len} fetched={len(rows)}")

            enq = 0
            for r in rows:
                title  = (r.get("title") or "").strip()
                url    = (r.get("url") or "").strip()
                date   = (r.get("date") or "").strip()
                domain = (r.get("domain") or "").strip().lower()
                lang_raw = (r.get("language") or "").strip().lower()

                # Detectar idioma si no viene del feed
                lang = normalize_lang(lang_raw) if lang_raw else normalize_lang(safe_detect_lang(title))

                dedup_key = url or f"{title}|{date}"
                if not dedup_key or dedup_key in seen:
                    continue
                seen.add(dedup_key)

                event = {
                    "headline": title or "(sin título)",
                    "summary": f"Fuente: {domain}" if domain else "",
                    "tickers": guess_tickers(title),
                    "category": classify(title),
                    "url": url,
                    "ts": to_iso_utc(date),
                    "domain": domain,
                    "language": lang,  # <<<<<< ESTE CAMPO ya llega al FRONT
                }
                await queue.put(event)
                enq += 1

            print(f"[GDELT] encoladas={enq}")

            # 2) (Opcional) Fallback Reuters si no llegó nada nuevo
            # Descomentar si querés fallback automáticamente
            # if enq == 0:
            #     rss_items = fetch_reuters_rss(limit=40)
            #     print(f"[RSS] fetched={len(rss_items)}")
            #     enq_rss = 0
            #     for it in rss_items:
            #         title = it["title"]; url = it["url"]
            #         dedup_key = url or f"{title}|{it.get('ts','')}"
            #         if not dedup_key or dedup_key in seen:
            #             continue
            #         seen.add(dedup_key)
            #         event = {
            #             "headline": title or "(sin título)",
            #             "summary": f"Fuente: {it.get('domain','reuters.com')}",
            #             "tickers": guess_tickers(title),
            #             "category": classify(title),
            #             "url": url,
            #             "ts": to_iso_utc(it.get("ts","")),
            #             "domain": it.get("domain","reuters.com"),
            #             "language": it.get("language","en"),
            #         }
            #         await queue.put(event)
            #         enq_rss += 1
            #     print(f"[RSS] encoladas={enq_rss}")

            backoff = POLL_INTERVAL_SEC

        except Exception as e:
            print("[POLL] error:", repr(e))
            backoff = min(backoff * 2, 300)

        await asyncio.sleep(backoff)
