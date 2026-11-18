"""
Hub em memória para Server-Sent Events por usuário.
Compatível com um único processo worker (ex.: Flask app.run ou gunicorn -w 1).
"""

from __future__ import annotations

import json
import queue
import threading
from typing import Any

_lock = threading.Lock()
_subscribers: dict[str, list[queue.Queue]] = {}


def subscribe(user_id: str) -> queue.Queue:
    q: queue.Queue = queue.Queue(maxsize=100)
    with _lock:
        _subscribers.setdefault(user_id, []).append(q)
    return q


def unsubscribe(user_id: str, q: queue.Queue) -> None:
    with _lock:
        lst = _subscribers.get(user_id)
        if not lst:
            return
        if q in lst:
            lst.remove(q)
        if not lst:
            del _subscribers[user_id]


def publish(user_id: str, event: dict[str, Any]) -> None:
    if not user_id:
        return
    data = json.dumps(event, ensure_ascii=False)
    with _lock:
        queues = list(_subscribers.get(user_id, []))
    for q in queues:
        try:
            q.put_nowait(data)
        except queue.Full:
            try:
                q.get_nowait()
            except queue.Empty:
                pass
            try:
                q.put_nowait(data)
            except queue.Full:
                pass
