from __future__ import annotations

import argparse
import json
import os
from typing import Any


def _emit(ok: bool, **extra: Any) -> int:
    payload: dict[str, Any] = {"ok": bool(ok)}
    payload.update(extra)
    print(json.dumps(payload, ensure_ascii=False))
    return 0 if ok else 2


def _probe(mode: str, symbols: list[str]) -> bool:
    import easy_xt

    api = easy_xt.get_api()
    if mode == "active":
        broker = easy_xt.get_xtquant_broker()
        for code in symbols:
            tick = broker.get_full_tick([code]) or {}
            info = tick.get(code) if isinstance(tick, dict) else None
            if isinstance(info, dict):
                price = float(info.get("lastPrice") or info.get("last_price") or info.get("price") or 0)
                if price > 0:
                    return True
    if hasattr(api, "data"):
        for code in symbols:
            try:
                price_df = api.data.get_current_price([code])
                if price_df is not None and getattr(price_df, "empty", False) is False:
                    return True
            except Exception:
                continue
    return False


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--mode", default="safe", choices=["active", "safe"])
    parser.add_argument("--symbols", default="000001.SZ,511090.SH")
    args = parser.parse_args()
    os.environ.setdefault("EASYXT_ENABLE_QMT_ONLINE", "0")
    symbols = [s.strip() for s in str(args.symbols).split(",") if s.strip()]
    try:
        ok = _probe(str(args.mode), symbols)
        return _emit(ok, mode=args.mode)
    except Exception as e:
        return _emit(False, mode=args.mode, error=str(e))


if __name__ == "__main__":
    raise SystemExit(main())
