# fragmento esencial de main.py
from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from starlette.staticfiles import StaticFiles
import asyncio

app = FastAPI()
news_queue: asyncio.Queue = asyncio.Queue()
poller_task = None

@app.on_event("startup")
async def startup():
    from .news_poller import poll_news
    global poller_task
    poller_task = asyncio.create_task(poll_news(news_queue))

@app.on_event("shutdown")
async def shutdown():
    if poller_task and not poller_task.done():
        poller_task.cancel()
        try:
            await poller_task
        except Exception:
            pass

@app.websocket("/ws/news")
async def ws_news(websocket: WebSocket):
    await websocket.accept()
    try:
        while True:
            event = await news_queue.get()
            await websocket.send_json(event)
    except WebSocketDisconnect:
        pass
    except Exception as e:
        try:
            await websocket.close()
        finally:
            print("WS error:", repr(e))

# Montar frontend (ajusta ruta si lo tenías distinto)
import pathlib
FRONT = pathlib.Path(__file__).parent.parent / "frontend"
app.mount("/", StaticFiles(directory=str(FRONT), html=True), name="static")
