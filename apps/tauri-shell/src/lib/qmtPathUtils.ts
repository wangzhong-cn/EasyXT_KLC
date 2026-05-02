import type { BrokerAccountPayload, QmtRuntimeConfigPayload } from "./api";

export function normalizeComparablePath(value: string | null | undefined): string {
  return String(value ?? "")
    .trim()
    .replace(/\\/g, "/")
    .replace(/\/+/g, "/")
    .replace(/\/$/, "")
    .toLowerCase();
}

export function pathsMostlyMatch(left: string | null | undefined, right: string | null | undefined): boolean {
  const a = normalizeComparablePath(left);
  const b = normalizeComparablePath(right);
  if (!a || !b) {
    return false;
  }
  return a === b || a.startsWith(`${b}/`) || b.startsWith(`${a}/`);
}

export function getBrokerAccountDisplayLabel(account: BrokerAccountPayload): string {
  if (account.label) {
    return account.label;
  }
  if (account.trade_account) {
    return account.trade_account;
  }
  return account.id;
}

export function runtimeConfigMatchesAccount(
  runtimeConfig: QmtRuntimeConfigPayload | null | undefined,
  account: BrokerAccountPayload,
): boolean {
  if (!runtimeConfig) {
    return false;
  }
  if (pathsMostlyMatch(runtimeConfig.qmt_userdata_path, account.qmt_userdata_path)) {
    return true;
  }
  return pathsMostlyMatch(runtimeConfig.qmt_path, account.qmt_exe_path);
}