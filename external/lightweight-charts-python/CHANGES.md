# EasyXT Modifications to lightweight-charts-python

This file records all changes made to the upstream fork
[`lightweight-charts-python`](https://github.com/louisnw01/lightweight-charts-python)
(MIT License, louisnw01) after it was incorporated into EasyXT_KLC.

Upstream version at fork point: **v2.1** (commit pinned in README).

---

## Changes

### Error handling — `lightweight_charts/widgets.py`

**Date:** 2025-Q1
**Author:** EasyXT team
**Change:** Wrapped `emit_callback()` in a `try/except Exception` block so that
errors in Python callbacks registered by user code no longer crash the Qt event
loop silently. Exceptions are now logged via `logging.getLogger(__name__).exception()`.

```python
# Before
def emit_callback(window, string):
    func, args = parse_event_message(window, string)
    asyncio.create_task(func(*args)) if asyncio.iscoroutinefunction(func) else func(*args)

# After
def emit_callback(window, string):
    try:
        func, args = parse_event_message(window, string)
        asyncio.create_task(func(*args)) if asyncio.iscoroutinefunction(func) else func(*args)
    except Exception:
        logging.getLogger(__name__).exception(
            "emit_callback error (message truncated): %s", string[:120]
        )
```

---

### Debug print removal — `lightweight_charts/drawings.py`

**Date:** 2025-Q1
**Author:** EasyXT team
**Change:** Removed a leftover debug `print()` call in `Drawing.update()` that
printed internal point coordinates to stdout on every drawing interaction.

```python
# Before (line ~33)
def update(self, points):
    print(f'{self.id}.updatePoints({points})')
    ...

# After
def update(self, points):
    ...
```

---

## Upstream License

The upstream library is licensed under the **MIT License**:

```
MIT License

Copyright (c) 2023 louisnw01

Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software"), to deal
in the Software without restriction, including without limitation the rights
to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in all
copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
SOFTWARE.
```

## Bundled Third-Party Asset

`lightweight_charts/js/lightweight-charts.js` is **TradingView Lightweight Charts™**
v4.1.3, licensed under the **Apache License 2.0**.
Copyright (c) 2024 TradingView, Inc.
See the root `NOTICE` file and `THIRD_PARTY_LICENSES.md` for the full attribution text
and attribution logo requirement.
