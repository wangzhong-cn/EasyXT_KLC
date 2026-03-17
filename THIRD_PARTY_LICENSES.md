# Third-Party Licenses

EasyXT_KLC incorporates components from third-party open-source projects.
This file lists them with their respective licenses.

---

## 1. TradingView Lightweight Charts™

| Field        | Value |
|--------------|-------|
| Component    | lightweight-charts |
| Version      | 4.1.3 (bundled as `lightweight-charts.js`) |
| Purpose      | Financial chart rendering engine |
| Source URL   | https://github.com/tradingview/lightweight-charts |
| License      | Apache License 2.0 |
| Copyright    | Copyright (c) 2024 TradingView, Inc. |

**Apache 2.0 Attribution Requirement:**
The "Powered by TradingView" watermark and link must remain visible in any
user-facing chart interface. Do not remove `attributionLogo: true` from chart
configuration.

Full license text: https://www.apache.org/licenses/LICENSE-2.0

---

## 2. lightweight-charts-python (fork base)

| Field        | Value |
|--------------|-------|
| Component    | lightweight-charts-python |
| Version      | 2.1 (forked, with modifications) |
| Purpose      | Python wrapper for lightweight-charts in Qt/PyQt |
| Source URL   | https://github.com/louisnw01/lightweight-charts-python |
| License      | MIT License |
| Copyright    | Copyright (c) 2023 louisnw01 |

**Note:** This project includes a fork of lightweight-charts-python with
modifications. Our changes are documented in:
`external/lightweight-charts-python/CHANGES.md`

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

---

## 3. pandas

| Field        | Value |
|--------------|-------|
| Component    | pandas |
| Purpose      | Data manipulation and analysis |
| Source URL   | https://github.com/pandas-dev/pandas |
| License      | BSD 3-Clause License |
| Copyright    | Copyright (c) 2008-2011, AQR Capital Management, LLC, Lambda Foundry, Inc. and PyData Development Team |

---

## 4. PyQt5

| Field        | Value |
|--------------|-------|
| Component    | PyQt5 |
| Purpose      | Qt5 Python bindings (GUI framework) |
| Source URL   | https://www.riverbankcomputing.com/software/pyqt/ |
| License      | GPL v3 / Commercial |
| Copyright    | Copyright (c) Riverbank Computing Limited |

**Note:** PyQt5 is licensed under GPL v3. If this software is distributed
commercially without source code disclosure, a PyQt5 commercial license is
required from Riverbank Computing.

---

## 5. DuckDB

| Field        | Value |
|--------------|-------|
| Component    | duckdb |
| Purpose      | In-process analytical database |
| Source URL   | https://github.com/duckdb/duckdb |
| License      | MIT License |
| Copyright    | Copyright (c) 2018-2024 DuckDB Foundation |

---

## 6. websockets

| Field        | Value |
|--------------|-------|
| Component    | websockets |
| Purpose      | WebSocket server for chart bridge communication |
| Source URL   | https://github.com/python-websockets/websockets |
| License      | BSD 3-Clause License |
| Copyright    | Copyright (c) Aymeric Augustin and contributors |

---

## Compliance Checklist

Before each public release, verify:

- [ ] `NOTICE` file is present in distribution root
- [ ] `THIRD_PARTY_LICENSES.md` is present in distribution root
- [ ] `LICENSE` file (Apache 2.0) is present for `lightweight-charts.js`
- [ ] TradingView `attributionLogo: true` is NOT disabled in chart config
- [ ] "Powered by TradingView" link is visible in chart UI
- [ ] No copyright headers removed from source files
- [ ] CI license gate (`tools/check_license.py`) passes
