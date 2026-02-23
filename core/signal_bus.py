import logging
from typing import Callable, Dict, List


logger = logging.getLogger(__name__)


class SignalBus:
    def __init__(self) -> None:
        self._subscribers: Dict[str, List[Callable[..., None]]] = {}

    def subscribe(self, event: str, handler: Callable[..., None]) -> None:
        handlers = self._subscribers.setdefault(event, [])
        if handler not in handlers:
            handlers.append(handler)

    def unsubscribe(self, event: str, handler: Callable[..., None]) -> None:
        handlers = self._subscribers.get(event, [])
        if handler in handlers:
            handlers.remove(handler)
        if not handlers and event in self._subscribers:
            del self._subscribers[event]

    def emit(self, event: str, **payload) -> None:
        handlers = list(self._subscribers.get(event, []))
        for handler in handlers:
            try:
                handler(**payload)
            except Exception:
                logger.exception("Signal handler error: %s", event)
