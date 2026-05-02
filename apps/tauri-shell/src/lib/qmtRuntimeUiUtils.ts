export function getQmtSessionTone(
  status: string | null | undefined,
): "ok" | "warning" | "danger" | "neutral" {
  switch (status) {
    case "healthy":
    case "connected":
      return "ok";
    case "login_pending":
    case "launching":
    case "process_alive":
      return "warning";
    case "degraded":
    case "quarantined":
      return "danger";
    default:
      return "neutral";
  }
}

export function getQmtConflictTone(severity: string | null | undefined): "ok" | "warning" | "danger" {
  switch (severity) {
    case "blocking":
      return "danger";
    case "manual_review_required":
    case "warning":
    default:
      return "warning";
  }
}