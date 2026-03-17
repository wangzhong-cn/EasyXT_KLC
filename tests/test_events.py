import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))

from core.events import Events


def test_events_constants():
    assert Events.SYMBOL_SELECTED == "symbol_selected"
    assert Events.ORDER_SUBMITTED == "order_submitted"
