import type { BrokerAccountPayload, QmtRuntimeConfigPayload } from "../../lib/api";
import { getBrokerAccountDisplayLabel } from "../../lib/qmtPathUtils";

interface QmtRuntimeConfigCardProps {
  runtimeConfig: QmtRuntimeConfigPayload | null;
  runtimeConfigReady: boolean;
  runtimeDefaultAccounts: BrokerAccountPayload[];
  onConfigureRequested?: () => void;
}

export function QmtRuntimeConfigCard({
  runtimeConfig,
  runtimeConfigReady,
  runtimeDefaultAccounts,
  onConfigureRequested,
}: QmtRuntimeConfigCardProps) {
  const shouldSuggestConfigure = !runtimeConfigReady || runtimeDefaultAccounts.length === 0;

  return (
    <div className="ifr-binding-draft-section">
      <div className="ifr-section-header">
        <div>
          <div className="wbd-data-section-title">运行时 QMT 主配置</div>
          <div className="ifr-section-note">当前写入 `config/unified_config.json` 的运行默认配置；下方账户可一键晋升为运行默认。</div>
        </div>
        {shouldSuggestConfigure && onConfigureRequested ? (
          <button type="button" className="ghost-button" onClick={onConfigureRequested}>
            快速配置 QMT
          </button>
        ) : null}
      </div>
      <div className="ifr-binding-draft-list">
        <div className="ifr-binding-draft-card">
          <div className="ifr-binding-draft-head">
            <div>
              <strong>unified_config 运行默认</strong>
              <div className="ifr-section-note">exe 与 userdata 任一命中即可视为当前运行默认账户。</div>
            </div>
            <div className="ifr-binding-draft-head-actions">
              <span className={`status-chip ${runtimeConfigReady ? "ok" : "warning"}`}>
                {runtimeConfigReady ? "已生效" : "未配置"}
              </span>
              <span className={`status-chip ${runtimeDefaultAccounts.length > 0 ? "ok" : "warning"}`}>
                {runtimeDefaultAccounts.length > 0 ? `命中 ${runtimeDefaultAccounts.length} 个账户` : "未对齐到账户"}
              </span>
              {runtimeConfig?.last_updated ? <span className="status-chip">{runtimeConfig.last_updated}</span> : null}
            </div>
          </div>
          <div className="ifr-binding-draft-grid">
            <div className="ifr-binding-draft-block">
              <div className="ifr-binding-draft-title">qmt_path</div>
              <div className="mono-text">{runtimeConfig?.qmt_path || "—"}</div>
            </div>
            <div className="ifr-binding-draft-block">
              <div className="ifr-binding-draft-title">qmt_userdata_path</div>
              <div className="mono-text">{runtimeConfig?.qmt_userdata_path || "—"}</div>
            </div>
            <div className="ifr-binding-draft-block">
              <div className="ifr-binding-draft-title">命中账户</div>
              <div className="ifr-binding-draft-reason-list">
                {runtimeDefaultAccounts.length > 0 ? runtimeDefaultAccounts.map((account) => (
                  <span key={`runtime-default-${account.id}`} className="status-chip ok">
                    {getBrokerAccountDisplayLabel(account)}
                  </span>
                )) : <span className="status-chip warning">当前未与任何账户路径对齐</span>}
              </div>
            </div>
          </div>
          <div className="ifr-binding-draft-note">
            这里是运行态单一事实源；账户绑定负责“账户 → 本地路径”，运行默认负责“当前主程序实际优先使用哪套 QMT 路径”。
          </div>
        </div>
      </div>
    </div>
  );
}