import logging
from typing import Callable

logger = logging.getLogger(__name__)


class SignalBus:
    def __init__(self) -> None:
        self._subscribers: dict[str, list[Callable[..., None]]] = {}

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

    def emit(self, event: str, **payload: object) -> None:
        handlers = list(self._subscribers.get(event, []))
        for handler in handlers:
            try:
                handler(**payload)
            except Exception:
                logger.exception("Signal handler error: %s", event)

    def request(self, event: str, **payload: object) -> list[object]:
        results: list[object] = []
        handlers = list(self._subscribers.get(event, []))
        for handler in handlers:
            try:
                results.append(handler(**payload))
            except Exception:
                logger.exception("Signal handler error: %s", event)
        return results


signal_bus = SignalBus()
