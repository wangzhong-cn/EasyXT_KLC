## Pyright 增量门禁（Type Safety Gate）

### 目标

- 不阻断历史存量告警
- 严格阻断 PR 引入的新告警
- baseline 更新必须可追溯、可审计、可授权

### 主工程扫描范围

- 配置文件: [pyrightconfig.main.json](file:///d:/EasyXT_KLC/pyrightconfig.main.json)

### 本地用法

运行严格门禁（新增告警即失败）：

```bash
python tools/pyright_incremental_gate.py --project pyrightconfig.main.json --baseline .ci/pyright_main_baseline.json --artifacts-dir artifacts --strict
```

更新 baseline（必须提供理由）：

```bash
python tools/pyright_incremental_gate.py --project pyrightconfig.main.json --baseline .ci/pyright_main_baseline.json --artifacts-dir artifacts --update-baseline --reason "#1234"
```

### CI 规则

- PR：
  - baseline 文件 `.ci/pyright_main_baseline.json` 若被修改，必须添加标签 `type-safety-baseline-approved`
- workflow_dispatch 更新 baseline：
  - 必须输入 `baseline_reason`（以 `#` 或 `http(s)://` 开头）
  - 触发者必须在白名单中

### 白名单配置（仓库变量）

在 GitHub 仓库 Settings → Variables 中新增：

- `BASELINE_UPDATE_ALLOWLIST`: 允许触发 baseline 更新的 GitHub 用户名列表
  - 以逗号或分号分隔，例如：`alice,bob;release-bot`

可用脚本快速配置：

```bash
powershell -ExecutionPolicy Bypass -File tools/set_baseline_allowlist.ps1 -Allowlist "alice,bob,release-bot" -Repo "owner/repo"
```

### 审计落盘

baseline JSON 将记录：

- `update_reason`
- `updated_by`
- `run_id`
- `ref`
