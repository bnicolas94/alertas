# backend/main.py
from __future__ import annotations

import asyncio
from pathlib import Path
from typing import Dict, List, Set

from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from fastapi.responses import FileResponse
from fastapi.staticfiles import StaticFiles

from .news_poller import poll_news

app = FastAPI(title="News Alert App")

# --------- Frontend estático ----------
FRONT_DIR = Path(__file__).resolve().parent.parent / "frontend"
app.mount("/static", StaticFiles(directory=str(FRONT_DIR)), name="static")

@app.get("/", include_in_schema=False)
def index():
    return FileResponse(FRONT_DIR / "index.html")

# --------- Estado global (WS + cola + historial) ----------
news_queue: asyncio.Queue = asyncio.Queue()

# Conexiones WebSocket actuales
clients: Set[WebSocket] = set()

# Historial (se mantiene en memoria)
MAX_HISTORY = 300  # cantidad máxima de tarjetas a recordar
_history: List[Dict] = []
_seen_keys: Set[str] = set()  # para deduplicar rápido dentro del historial


def _dedup_key(evt: Dict) -> str:
    title = (evt.get("headline") or "").strip()
    ts    = (evt.get("ts") or evt.get("timestamp") or "").strip()
    url   = (evt.get("url") or "").strip()
    return url or f"{title}|{ts}"


def _push_history(evt: Dict) -> None:
    """Agrega al historial con deduplicación y límite."""
    key = _dedup_key(evt)
    if not key or key in _seen_keys:
        return
    _history.append(evt)
    _seen_keys.add(key)
    # recortar si nos pasamos
    while len(_history) > MAX_HISTORY:
        old = _history.pop(0)
        _seen_keys.discard(_dedup_key(old))


async def _broadcast(evt: Dict) -> None:
    """Envía el evento a todos los clientes conectados."""
    dead: List[WebSocket] = []
    for ws in list(clients):
        try:
            await ws.send_json(evt)
        except Exception:
            dead.append(ws)
    for ws in dead:
        try:
            clients.remove(ws)
        except KeyError:
            pass


async def broadcaster_task():
    """Toma eventos de la cola, los guarda en historial y los emite a clientes."""
    while True:
        evt = await news_queue.get()
        _push_history(evt)
        await _broadcast(evt)


@app.on_event("startup")
async def _on_startup():
    # Lanza el poller que pone eventos en la cola
    app.state.poller_task = asyncio.create_task(poll_news(news_queue))
    # Lanza el broadcaster que reparte a los clientes y mantiene historial
    app.state.broadcast_task = asyncio.create_task(broadcaster_task())


@app.on_event("shutdown")
async def _on_shutdown():
    # Cancelar tareas en un shutdown ordenado
    for name in ("poller_task", "broadcast_task"):
        task = getattr(app.state, name, None)
        if task:
            task.cancel()
            try:
                await task
            except Exception:
                pass


@app.websocket("/ws/news")
async def ws_news(ws: WebSocket):
    await ws.accept()
    clients.add(ws)

    # 1) Enviar primero el HISTORIAL existente (en orden del más viejo al más nuevo)
    #    Si preferís solo los últimos K, cambiá el slice.
    try:
        for item in _history[-MAX_HISTORY:]:
            await ws.send_json(item)
    except Exception:
        # si falla al enviar el historial, cerramos la conexión
        try:
            clients.remove(ws)
        except KeyError:
            pass
        await ws.close()
        return

    # 2) Mantener la conexión viva; no esperamos datos del cliente,
    #    solo detectamos el cierre.
    try:
        while True:
            # No esperamos mensajes "útiles"; esto solo sirve para detectar desconexión.
            await ws.receive_text()
    except WebSocketDisconnect:
        pass
    finally:
        try:
            clients.remove(ws)
        except KeyError:
            pass
