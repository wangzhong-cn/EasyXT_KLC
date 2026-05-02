import type { ChartBar, ChartInterval } from "../chart/chartFacadeV2";

export interface ApiResponseMetaPayload {
  server_time: number;
  build_version?: string;
  commit_sha?: string;
}

export interface SystemStatePayload {
  state_root: string;
  catalog_path: string | null;
  sqlite_logical_seq: number;
  active_shard_id: string | null;
  active_shard_count: number;
  duckdb_shadow_version: string | null;
  sync_status: string;
  last_good_version: string | null;
  shadow_failed_stage: string | null;
  shadow_error: string | null;
  backup_last_success_at: string | null;
  shadow_manifest_path: string | null;
  federation_attach_budget: number;
  federation_executor_ready: boolean;
  server_time: number;
  build_version?: string;
  commit_sha?: string;
}

export interface SystemFrontendEventItem {
  event_id: string | null;
  event_ts: string | null;
  event_type: string | null;
  payload: unknown;
  raw_payload_json: string | null;
}

export interface SystemFrontendEventsPayload {
  configured: boolean;
  family_registered: boolean;
  state_root: string;
  source: string;
  items: SystemFrontendEventItem[];
  returned: number;
  latest_logical_seq: number | null;
  attached_shards: number;
  filters: {
    event_type: string;
    start_time: string;
    end_time: string;
    limit: number;
  };
  server_time: number;
  build_version?: string;
  commit_sha?: string;
}

export interface StructurePointPayload {
  ts: number | null;
  price: number | null;
}

export interface StructureLayer4Payload {
  attractor_mean: number | null;
  attractor_std: number | null;
  bayes_lower: number | null;
  bayes_upper: number | null;
  posterior_mean: number | null;
  observation_count: number | null;
  continuation_count: number | null;
  reversal_count: number | null;
  bayes_group_level: string | null;
  bayes_group_key: string | null;
}

export interface StructureItemPayload {
  structure_id: string;
  code: string;
  interval: string;
  created_at: number | null;
  direction: string | null;
  status: string | null;
  closed_at: number | null;
  retrace_ratio: number | null;
  layer4: StructureLayer4Payload;
  points: {
    p0: StructurePointPayload;
    p1: StructurePointPayload;
    p2: StructurePointPayload;
    p3: StructurePointPayload;
  };
}

export interface StructuresPayload {
  items: StructureItemPayload[];
  returned: number;
  limit: number;
  offset: number;
  filters: {
    code: string;
    interval: string;
    direction: string;
    status: string;
    include_bayes_meta: boolean;
    group_strategy: string;
    min_observations: number;
  };
  server_time: number;
}

export interface SignalRiskPayload {
  stop_loss_price: number | null;
  stop_loss_distance: number | null;
  drawdown_pct: number | null;
  calmar_snapshot: number | null;
}

export interface StructuredSignalItemPayload {
  signal_id: string;
  structure_id: string;
  code: string;
  interval: string;
  signal_ts: number | null;
  signal_type: string | null;
  trigger_price: number | null;
  risk: SignalRiskPayload;
  remarks: string | null;
}

export interface StructuredSignalsPayload {
  items: StructuredSignalItemPayload[];
  returned: number;
  limit: number;
  offset: number;
  filters: {
    structure_id: string;
    code: string;
    interval: string;
    signal_type: string;
  };
  server_time: number;
}

export interface BayesianSummaryItemPayload {
  bayes_group_level: string | null;
  bucket_count: number | null;
  structure_count: number | null;
  mean_posterior_mean: number | null;
  mean_observation_count: number | null;
  mean_retrace_ratio: number | null;
  reversed_ratio: number | null;
  mean_audit_event_count: number | null;
  mean_extend_event_count: number | null;
  mean_reverse_event_count: number | null;
}

export interface BayesianSummaryPayload {
  items: BayesianSummaryItemPayload[];
  returned: number;
  dataset_rows: number;
  group_by: string[];
  group_strategy: string;
  min_observations: number;
  filters: {
    code: string;
    interval: string;
    direction: string;
    status: string[];
    signal_type: string[];
  };
  server_time: number;
}

export interface StructureAuditItemPayload {
  audit_id: string;
  structure_id: string;
  code: string;
  interval: string;
  event_type: string | null;
  event_ts: number | null;
  snapshot: {
    direction?: string | null;
    status?: string | null;
    retrace_ratio?: number | null;
  } | null;
}

export interface StructureDetailPayload {
  structure: StructureItemPayload;
  latest_signal: StructuredSignalItemPayload | null;
  audit_items: StructureAuditItemPayload[];
  audit_summary: {
    audit_event_count: number | null;
    create_event_count: number | null;
    extend_event_count: number | null;
    reverse_event_count: number | null;
    last_event_ts: number | null;
    last_event_type: string | null;
  };
  filters: {
    audit_limit: number;
    include_bayes_meta: boolean;
    group_strategy: string;
    min_observations: number;
  };
  server_time: number;
}

export interface ApplyBayesianPayload {
  items: Array<Record<string, unknown>>;
  returned: number;
  dataset_rows: number;
  updated: number;
  group_by: string[];
  group_strategy: string;
  min_observations: number;
  writeback: boolean;
  filters: {
    code: string;
    interval: string;
    direction: string;
    status: string[];
    signal_type: string[];
  };
  server_time: number;
}

export interface DataRoutingMetricPayload {
  hits: number;
  misses: number;
  errors: number;
  quality_rejects: number;
  last_latency_ms: number | null;
  health?: Record<string, unknown>;
}

export interface DataPipelineCheckPayload {
  healthy?: boolean;
  [key: string]: unknown;
}

export interface DataEnvironmentConfigItemPayload {
  key: string;
  status: string;
  value: string;
  description: string;
  required: boolean;
  sensitive?: boolean;
}

export interface DataEnvironmentConfigPayload {
  groups: Record<string, DataEnvironmentConfigItemPayload[]>;
  summary: {
    total: number;
    configured: number;
    missing_required: number;
  };
  overall_valid: boolean;
}

export interface DataGovernanceOverviewPayload {
  datasource_health: {
    status: string;
    checks: {
      sources?: Record<string, Record<string, unknown>>;
      quarantine?: Record<string, unknown>;
      data_quality_incident?: Record<string, unknown>;
      step6_validation?: Record<string, unknown>;
      thresholds?: Record<string, unknown>;
      error?: string;
    };
    server_time: number;
    build_version?: string;
    commit_sha?: string;
  };
  sla_health: {
    status: string;
    sla?: Record<string, unknown>;
    error?: string;
    server_time: number;
    build_version?: string;
    commit_sha?: string;
  };
  pipeline: {
    overall_healthy: boolean;
    timestamp?: string;
    checks: Record<string, DataPipelineCheckPayload>;
    error?: string;
  };
  routing: {
    sources: Record<string, DataRoutingMetricPayload>;
    total_sources: number;
    healthy_sources: number;
    error?: string;
  };
  duckdb: {
    healthy: boolean;
    path?: string;
    table_count?: number;
    stock_daily_rows?: number;
    latest_date?: string;
    error?: string;
  };
  environment: DataEnvironmentConfigPayload;
  realtime: {
    connected: boolean | null;
    degraded: boolean;
    symbol: string;
    quote_ts: string;
    reason: string;
    drop_rate: number;
    total_quotes: number;
    queue_depth: number;
    source: string;
    error?: string;
  };
  receipts: {
    store: Record<string, number>;
    publish_gate: Record<string, unknown>;
    gate_reject_reasons: Record<string, number>;
    gate_reject_severity: Record<string, number>;
    gate_sla_impact: Record<string, number>;
    sla_threshold_panel: {
      status: string;
      thresholds: Record<string, number>;
      current: Record<string, number>;
      breaches: Record<string, boolean>;
    };
    sla_threshold_overrides: Record<string, number>;
    sla_threshold_config_meta?: ConfigMetaPayload;
    sla_threshold_version?: number;
    sla_threshold_updated_by?: string;
    sla_threshold_note?: string;
    action_rulebook: GovernanceActionRulePayload[];
    action_rulebook_meta?: ConfigMetaPayload;
    action_rulebook_validation?: GovernanceActionRuleValidationPayload;
    action_recommendations: GovernanceActionRecommendationPayload[];
    action_audit_recent: GovernanceActionAuditRecordPayload[];
    action_audit_meta?: ConfigMetaPayload;
    timeline: ReceiptTimelineItemPayload[];
    trend_7d: GateTrendPointPayload[];
    trend_by_symbol_7d: GateDimensionTrendPointPayload[];
    trend_by_period_7d: GateDimensionTrendPointPayload[];
  };
  summary: {
    datasource_status: string;
    sla_status: string;
    pipeline_healthy: boolean;
    healthy_sources: number;
    total_sources: number;
    duckdb_healthy: boolean;
    env_valid: boolean;
    realtime_connected: boolean | null;
    gate_degraded?: number;
    gate_reject_total?: number;
    gate_critical?: number;
    gate_warning?: number;
    sla_gate_block?: number;
    sla_monitor?: number;
    repair_receipts?: number;
    replay_receipts?: number;
  };
  filters: { sla_report_date: string; trend_days: number };
  server_time: number;
  build_version?: string;
  commit_sha?: string;
}

export interface DataGovernanceTradingCalendarPayload {
  start_date: string;
  end_date: string;
  total_days: number;
  trading_days: number;
  non_trading_days: number;
  trading_days_list: string[];
  non_trading_list: string[];
  weekend_days: number;
  holiday_days: number;
  error?: string;
  server_time: number;
  build_version?: string;
  commit_sha?: string;
}

export interface DataTraceabilityRecordPayload {
  stock_code: string;
  period: string;
  source: string;
  status: string;
  source_grade?: string | null;
  contract_pass?: boolean | null;
  cross_source_pass?: boolean | null;
  tick_verified?: boolean | null;
  lineage_complete?: boolean | null;
  replayable?: boolean | null;
  record_count?: number;
  start_date?: string;
  end_date?: string;
  last_updated?: string;
  ingest_run_id?: string;
  error_message?: string | null;
  gate_receipt_id?: string | null;
  quality_grade?: string | null;
  gate_reject_reason?: string | null;
}

export interface DataGovernanceTraceabilityPayload {
  records: DataTraceabilityRecordPayload[];
  total: number;
  error?: string | null;
  filters: {
    stock_code: string;
    period: string;
    limit: number;
  };
  server_time: number;
  build_version?: string;
  commit_sha?: string;
}

export interface Golden1dRepairTaskPayload {
  stock_code: string;
  period: string;
  start_date: string;
  end_date: string;
  reason: string;
  priority_hint: number | null;
  current_symbol: string;
  gap_length: number | null;
}

export interface Golden1dRepairSnapshotPayload {
  plan_status: string;
  generated_at: string | null;
  queued_tasks: number;
  failed_tasks: number;
  task_count: number;
  blocker_issues: string[];
  notes: string[];
  tasks: Golden1dRepairTaskPayload[];
}

export interface Golden1dRepairPlanPayload extends Golden1dRepairSnapshotPayload, ApiResponseMetaPayload {
  symbol: string;
}

export interface Golden1dRepairPlanListPayload extends ApiResponseMetaPayload {
  items: Golden1dRepairPlanPayload[];
  returned: number;
  limit: number;
}

export interface ReceiptHistoryItemPayload {
  receipt_id: string;
  stock_code: string;
  period: string;
  range_start?: string | null;
  range_end?: string | null;
  reason?: string | null;
  source?: string | null;
  status?: string | null;
  task_count?: number | null;
  queued_tasks?: number | null;
  failed_tasks?: number | null;
  lineage_anchor?: string | null;
  related_gate_receipt_id?: string | null;
  related_repair_receipt_id?: string | null;
  related_replay_receipt_id?: string | null;
  replay_kind?: string | null;
  last_error?: string | null;
  tick_verified?: boolean | null;
  replayable?: boolean | null;
  created_at?: string | null;
}

export interface ReceiptHistoryPayload {
  receipt_type: "publish_gate" | "repair" | "replay";
  items: ReceiptHistoryItemPayload[];
  returned: number;
  limit: number;
  server_time: number;
}

export interface ReceiptTimelineItemPayload {
  receipt_type: "publish_gate" | "repair" | "replay";
  receipt_id: string;
  stock_code: string;
  period: string;
  range_start?: string | null;
  range_end?: string | null;
  status?: string | null;
  result_status?: string | null;
  gate_reject_reason?: string | null;
  severity?: string | null;
  sla_impact?: string | null;
  lineage_anchor?: string | null;
  related_repair_receipt_id?: string | null;
  related_replay_receipt_id?: string | null;
  related_gate_receipt_id?: string | null;
  created_at?: string | null;
}

export interface ReceiptTimelinePayload {
  items: ReceiptTimelineItemPayload[];
  returned: number;
  filters: {
    symbol: string;
    period: string;
    lineage_anchor: string;
    receipt_type?: string;
    gate_reject_reason?: string;
    severity?: string;
    lookback_days?: number;
    limit: number;
  };
  server_time: number;
}

export interface GateTrendPointPayload {
  trade_day: string;
  total: number;
  golden: number;
  degraded: number;
  rejected: number;
  tick_mismatch: number;
  cross_source_conflict: number;
}

export interface GateDimensionTrendPointPayload {
  trade_day: string;
  dimension_value: string;
  dimension: string;
  total: number;
  rejected: number;
  critical: number;
  warning: number;
}

export interface LineageAnchorDetailPayload {
  lineage_anchor: string;
  symbols: string[];
  periods: string[];
  receipt_counts: Record<string, number>;
  timeline: ReceiptTimelineItemPayload[];
  traceability_records?: DataTraceabilityRecordPayload[];
  latest_receipt_id?: string | null;
  latest_status?: string | null;
  server_time: number;
}

export interface GovernanceActionRulePayload {
  rule_id: string;
  match_reason: string;
  severity: string;
  sla_impact: string;
  recommended_action: string;
  business_meaning: string;
}

export interface GovernanceActionRuleValidationPayload {
  valid: boolean;
  errors: string[];
  rule_count: number;
  required_fields: string[];
}

export interface GovernanceActionRecommendationPayload {
  action_id: string;
  tone: string;
  title: string;
  detail: string;
  action_type: string;
  payload: Record<string, unknown>;
}

export interface GovernanceSlaThresholdPayload {
  overrides: Record<string, number>;
  panel: {
    status: string;
    thresholds: Record<string, number>;
    current: Record<string, number>;
    breaches: Record<string, boolean>;
  };
  config_meta?: ConfigMetaPayload;
  config_version?: number;
  updated_by?: string;
  note?: string;
  audit_record?: GovernanceActionAuditRecordPayload;
  server_time: number;
}

export interface ConfigMetaPayload {
  path: string;
  exists: boolean;
  updated_at?: string | null;
  size_bytes?: number;
  config_version?: number;
  updated_by?: string;
  note?: string;
  version?: string;
  maintainer?: string;
}

export interface GovernanceActionAuditRecordPayload {
  event_id: string;
  event_time: string;
  action_id: string;
  action_type: string;
  tone: string;
  title: string;
  detail: string;
  source: string;
  stock_code?: string;
  period?: string;
  lineage_anchor?: string;
  operator?: string;
  config_version?: number | null;
  payload: Record<string, unknown>;
}

export interface GovernanceActionAuditPayload {
  records?: GovernanceActionAuditRecordPayload[];
  record?: GovernanceActionAuditRecordPayload;
  returned?: number;
  filters?: {
    limit: number;
    action_type: string;
    source: string;
    stock_code?: string;
    period?: string;
    lineage_anchor?: string;
  };
  config_meta?: ConfigMetaPayload;
  server_time: number;
}

export interface GovernanceSnapshotExportPayload {
  snapshot_name: string;
  generated_at: string;
  overview: DataGovernanceOverviewPayload;
  action_audit: GovernanceActionAuditRecordPayload[];
  config_sources: {
    sla_thresholds: ConfigMetaPayload;
    action_rulebook: ConfigMetaPayload;
    action_audit: ConfigMetaPayload;
  };
  server_time: number;
}

export type DataIngestionJobStatus = "queued" | "running" | "cancelling" | "cancelled" | "completed" | "failed";

export interface DataIngestionJobRequestPayload {
  stock_codes_count: number;
  stock_codes_preview: string[];
  request_mode?: string;
  requested_exchanges?: string[];
  use_all_stocks?: boolean;
  periods: string[];
  start_date: string | null;
  end_date: string | null;
  precompute_after_download: boolean;
  enable_audit_after_ingest?: boolean;
  download_workers?: number | null;
  intraday_lookback_days?: number | null;
  stage_label?: string | null;
}

export interface DataIngestionJobProgressPayload {
  current: number;
  total: number;
  processed_stocks: number;
  current_stock_code: string | null;
  current_period: string | null;
  last_status: string | null;
  message: string;
  updated_at: string | null;
}

export interface DataIngestionJobSummaryPayload {
  processed_stocks: number;
  success_stocks: number;
  failed_stocks: number;
  total_records: number;
  audit_passed_stocks: number;
  audit_failed_stocks: number;
  repair_queued_stocks: number;
  source_breakdown?: Record<string, number>;
  source_breakdown_by_period?: Record<string, Record<string, number>>;
}

export interface DataIngestionJobPayload extends ApiResponseMetaPayload {
  job_id: string;
  status: DataIngestionJobStatus;
  created_at: string;
  started_at: string | null;
  finished_at: string | null;
  request: DataIngestionJobRequestPayload;
  progress: DataIngestionJobProgressPayload;
  summary: DataIngestionJobSummaryPayload;
  error: string | null;
  message?: string;
}

export interface DataIngestionJobListPayload extends ApiResponseMetaPayload {
  items: DataIngestionJobPayload[];
  returned: number;
  filters: {
    status_filter: string;
    limit: number;
  };
}

export interface DataIngestionJobCreateInput {
  stockCodes?: string[];
  useAllStocks?: boolean;
  exchanges?: string[];
  periods?: string[];
  startDate?: string;
  endDate?: string;
  precomputeAfterDownload?: boolean;
  enableAuditAfterIngest?: boolean;
  downloadWorkers?: number;
  intradayLookbackDays?: number;
  stageLabel?: string;
}

export type CoverageMatrixJobStatus = "queued" | "running" | "cancelling" | "cancelled" | "completed" | "failed";
export type CoverageMatrixArtifactKey = "json" | "summary" | "heatmap_csv" | "heatmap_png";

export interface CoverageMatrixConfigSymbolPayload {
  symbol: string;
  name: string;
}

export interface CoverageMatrixConfigGroupPayload {
  id: string;
  asset_class: string;
  symbol_count: number;
  symbols: CoverageMatrixConfigSymbolPayload[];
}

export interface CoverageMatrixConfigWindowPayload {
  id: string;
  start: string;
  end: string;
  regime: string;
}

export interface CoverageMatrixConfigPayload {
  config_path: string;
  config_name: string;
  anchor_period: string;
  task_count: number;
  group_count: number;
  symbol_count: number;
  periods: number[];
  windows: CoverageMatrixConfigWindowPayload[];
  groups: CoverageMatrixConfigGroupPayload[];
  runtime_defaults: {
    workers: number;
    resume: boolean;
    tolerance_tick: number;
    split_ratio: number;
    duckdb_path: string | null;
  };
}

export interface CoverageMatrixByGroupItemPayload {
  group: string;
  total: number;
  passes: number;
  pass_ratio: number;
}

export interface CoverageMatrixBySymbolPeriodItemPayload {
  group: string;
  symbol: string;
  intraday_period: number;
  score: number;
}

export interface CoverageMatrixIssueBreakdownItemPayload {
  status: string;
  reason: string;
  count: number;
  symbols: string[];
  windows: string[];
  periods: number[];
}

export interface CoverageMatrixReportSummaryPayload {
  generated_at: string | null;
  config_meta: {
    config_name?: string;
    anchor_period?: string;
    periods?: number[];
    symbols?: string[];
    windows?: string[];
  };
  totals: {
    total_tasks: number;
    configured_tasks?: number;
    remaining_tasks?: number;
    pass_tasks: number;
    partial_tasks: number;
    fail_tasks: number;
    error_tasks: number;
    unknown_tasks: number;
    pass_ratio: number;
  };
  stopped_early: boolean;
  by_group: CoverageMatrixByGroupItemPayload[];
  by_symbol_period: CoverageMatrixBySymbolPeriodItemPayload[];
  issue_breakdown: CoverageMatrixIssueBreakdownItemPayload[];
  artifacts: Partial<Record<CoverageMatrixArtifactKey, string>>;
  output_dir?: string | null;
  source?: string;
}

export interface CoverageMatrixJobRequestPayload {
  config_path: string;
  output_dir: string;
  workers: number;
  resume: boolean;
  config_name: string;
  task_count: number;
  periods: number[];
  windows: string[];
  groups: string[];
}

export interface CoverageMatrixJobProgressPayload {
  completed_tasks: number;
  total_tasks: number;
  current_task_id: string | null;
  current_symbol: string | null;
  current_period: string | null;
  current_window_id: string | null;
  last_status: string | null;
  message: string;
  updated_at: string | null;
}

export interface CoverageMatrixJobSummaryPayload {
  completed_tasks: number;
  configured_tasks: number;
  pass_tasks: number;
  partial_tasks: number;
  fail_tasks: number;
  error_tasks: number;
  unknown_tasks: number;
  pass_ratio: number;
  remaining_tasks: number;
}

export interface CoverageMatrixJobPayload extends ApiResponseMetaPayload {
  job_id: string;
  status: CoverageMatrixJobStatus;
  created_at: string;
  started_at: string | null;
  finished_at: string | null;
  request: CoverageMatrixJobRequestPayload;
  progress: CoverageMatrixJobProgressPayload;
  summary: CoverageMatrixJobSummaryPayload;
  artifacts: Partial<Record<CoverageMatrixArtifactKey, string>>;
  latest_report: CoverageMatrixReportSummaryPayload | null;
  error: string | null;
  message?: string;
}

export interface CoverageMatrixJobListPayload extends ApiResponseMetaPayload {
  items: CoverageMatrixJobPayload[];
  returned: number;
  filters: {
    status_filter: string;
    limit: number;
  };
}

export interface CoverageMatrixOverviewPayload extends ApiResponseMetaPayload {
  config: CoverageMatrixConfigPayload | null;
  config_error: string | null;
  latest_report: CoverageMatrixReportSummaryPayload | null;
  recent_jobs: CoverageMatrixJobPayload[];
  returned_jobs: number;
}

export interface CoverageMatrixJobCreateInput {
  configPath?: string;
  outputDir?: string;
  workers?: number;
  resume?: boolean;
}

export interface DownloadedFilePayload {
  blob: Blob;
  filename: string;
  contentType: string;
}

export interface Golden1dRepairTriggerPayload extends ApiResponseMetaPayload {
  symbol: string;
  status: string;
  queued_tasks: number;
  failed_tasks: number;
  blocker_issues: string[];
  notes: string[];
  force_full: boolean;
  repair: Golden1dRepairSnapshotPayload;
}

// Golden 1D 批量状态快照（只读，从 audit DB 聚合）
export interface Golden1dBatchStatusPayload extends ApiResponseMetaPayload {
  total_audited: number;
  golden_count: number;
  partial_trust_count: number;
  degraded_count: number;
  unknown_count: number;
  golden_ratio: number;
  last_batch_audited_at: string | null;
}

// Golden 1D 批量 audit 触发结果
export interface Golden1dBatchAuditResultPayload extends ApiResponseMetaPayload {
  total_audited: number;
  golden_count: number;
  partial_trust_count: number;
  degraded_count: number;
  unknown_count: number;
  stored_symbols_count?: number;
  coverage_note?: string | null;
  audited_at?: string | null;
  force_full: boolean;
  limit: number;
}

export interface PeriodAssetItemPayload {
  period: string;
  period_code: string;
  label: string;
  period_family: string;
  base_source: string;
  alignment: string;
  anchor: string;
  layer: string;
  precompute_default: boolean;
  ui_visible_default: boolean;
  tick_verifiable: boolean;
  covered_symbols: number;
  total_bars: number;
  earliest_bar: string | null;
  latest_bar: string | null;
  last_indexed_at: string | null;
}

export interface PeriodAssetMatrixPayload extends ApiResponseMetaPayload {
  periods: PeriodAssetItemPayload[];
  total_periods: number;
  duckdb_error: string | null;
}

export interface DbTableColumnPayload {
  name: string;
  type: string;
  nullable: boolean;
}

export interface DbTableSummaryPayload {
  table_name: string;
  row_count: number;
  column_count: number;
  columns: DbTableColumnPayload[];
  time_column: string | null;
  symbol_column: string | null;
  distinct_symbols: number | null;
  earliest_value: string | number | boolean | null;
  latest_value: string | number | boolean | null;
}

export interface DbTablesOverviewPayload extends ApiResponseMetaPayload {
  database: {
    path: string;
    exists: boolean;
    file_size_bytes: number;
    modified_at: string | null;
    table_count: number;
  };
  items: DbTableSummaryPayload[];
  returned: number;
  include_columns: boolean;
  include_empty: boolean;
}

export interface DbTableRowsPayload extends ApiResponseMetaPayload {
  database_path: string;
  table_name: string;
  columns: string[];
  column_details: DbTableColumnPayload[];
  rows: Array<Record<string, string | number | boolean | null>>;
  returned: number;
  limit: number;
  offset: number;
  total_rows: number;
}

export interface DbQueryResultPayload extends ApiResponseMetaPayload {
  database_path: string;
  sql: string;
  bounded_sql: string;
  columns: string[];
  rows: Array<Record<string, string | number | boolean | null>>;
  returned: number;
  limit: number;
  truncated: boolean;
}

export interface DbCsvImportResultPayload extends ApiResponseMetaPayload {
  database_path: string;
  table_name: string;
  mode: "append" | "replace";
  inserted_rows: number;
  columns_written: string[];
}

export interface LateEventReplayTriggerPayload {
  symbol: string;
  period: string;
  result: {
    processed: number;
    succeeded: number;
    failed: number;
    dead_letter: number;
  };
  limit: number;
  max_retries: number;
  reason_regex: string;
  audit_record?: GovernanceActionAuditRecordPayload;
  server_time: number;
}

export interface ChartBarsQualityPayload {
  golden_status: "golden" | "partial_trust" | "degraded" | "unknown";
  is_golden_1d_ready: boolean;
  missing_days: number | null;
  cross_source_status: string;
  backfill_status: string;
  last_audited_at: string | null;
  audit_anchor_date: string | null;
  listing_date: string | null;
  listing_date_confidence: "verified" | "fallback" | "unknown";
  issues: string[];
  repair: Golden1dRepairSnapshotPayload;
}

export interface IngestionGateStatusPayload {
  stock_code: string;
  period: string;
  start_date: string | null;
  end_date: string | null;
  status: string | null;
  record_count: number | null;
  gate_receipt_id: string | null;
  threshold_version: string | null;
  session_profile_version: string | null;
  timestamp_contract_version: string | null;
  period_registry_version: string | null;
  source_grade: "golden" | "partial_trust" | "degraded" | "unknown" | string | null;
  contract_pass: boolean | null;
  cross_source_pass: boolean | null;
  tick_verified: boolean | null;
  lineage_complete: boolean | null;
  replayable: boolean | null;
  quality_grade: "golden" | "partial_trust" | "degraded" | "unknown" | string | null;
  gate_reject_reason?: string | null;
  last_updated: string | null;
  server_time: number;
  build_version?: string;
  commit_sha?: string;
}

export interface MarketBarSubscriptionPayload {
  symbol: string;
  period: string;
  subscribed_at: number;
  ingested_count: number;
  error_count: number;
  last_tick_ts: number | null;
}

export interface MarketBarSubscriptionsStatsPayload {
  total_subscriptions?: number;
  total_bar_subscriptions?: number;
  total_ingested?: number;
  total_errors?: number;
  qmt_available?: boolean;
}

export interface MarketBarSubscriptionsResponsePayload {
  subscriptions: MarketBarSubscriptionPayload[];
  stats: MarketBarSubscriptionsStatsPayload;
}

export interface ChartBarsPayload {
  symbol: string;
  interval: ChartInterval | string;
  resolved_period: string;
  adjust: string;
  start_date: string;
  end_date: string;
  bar_count: number;
  bars: ChartBar[];
  quality: ChartBarsQualityPayload;
  server_time: number;
  build_version?: string;
  commit_sha?: string;
}

export interface ChartPeriodItemPayload {
  code: string;
  label: string;
  period_code: string;
  runtime_code: string;
  aliases: string[];
  layer: string;
  period_family: string;
  base_source: string;
  alignment: string;
  anchor: string;
  precompute_default: boolean;
  ui_visible_default: boolean;
  supports_partial: boolean;
  tick_verifiable: boolean;
  enabled: boolean;
  description: string;
}

export interface ChartPeriodCatalogPayload {
  registry_version: string;
  default_interval: string;
  quick_intervals: string[];
  items: ChartPeriodItemPayload[];
  server_time: number;
  build_version?: string;
  commit_sha?: string;
}

export interface ChartBarsQueryParams {
  symbol: string;
  interval: ChartInterval;
  /** 日/周线游标，格式 YYYY-MM-DD */
  startDate?: string;
  /** 日/周线游标，格式 YYYY-MM-DD */
  endDate?: string;
  /**
   * 分钟/小时线游标（精确），格式 "YYYY-MM-DD HH:MM:SS"（交易所本地时间）。
   * 优先级高于 startDate；若同时传入则 startDatetime 生效。
   */
  startDatetime?: string;
  /**
   * 分钟/小时线游标（精确），格式 "YYYY-MM-DD HH:MM:SS"（交易所本地时间）。
   * 优先级高于 endDate；若同时传入则 endDatetime 生效。
   */
  endDatetime?: string;
  adjust?: string;
  limit?: number;
}

const DEFAULT_API_BASE = (import.meta.env.VITE_EASYXT_API_BASE as string | undefined)?.trim()
  || "http://127.0.0.1:8765";
const API_TOKEN = (import.meta.env.VITE_EASYXT_API_TOKEN as string | undefined)?.trim() || "";

export function getApiBaseUrl(): string {
  return DEFAULT_API_BASE;
}

// ---------------------------------------------------------------------------
// 会话令牌管理 (localStorage 持久化)
// ---------------------------------------------------------------------------

const AUTH_TOKEN_KEY = "easyxt_auth_token";
const AUTH_USER_KEY = "easyxt_auth_user";

export function getAuthToken(): string {
  return localStorage.getItem(AUTH_TOKEN_KEY) || "";
}

export function setAuthToken(token: string): void {
  localStorage.setItem(AUTH_TOKEN_KEY, token);
}

export function clearAuthToken(): void {
  localStorage.removeItem(AUTH_TOKEN_KEY);
  localStorage.removeItem(AUTH_USER_KEY);
}

export function getStoredUser(): AuthUser | null {
  const raw = localStorage.getItem(AUTH_USER_KEY);
  if (!raw) return null;
  try {
    return JSON.parse(raw) as AuthUser;
  } catch {
    return null;
  }
}

export function setStoredUser(user: AuthUser): void {
  localStorage.setItem(AUTH_USER_KEY, JSON.stringify(user));
}

function buildApiUrl(path: string): string {
  return `${DEFAULT_API_BASE.replace(/\/$/, "")}${path}`;
}

function normalizeNetworkError(error: unknown, url: string): Error {
  const message = error instanceof Error ? error.message : String(error ?? "");
  if (/failed to fetch|networkerror|err_connection_refused|load failed/i.test(message)) {
    return new Error(`后端服务不可达：${url}`);
  }
  return error instanceof Error ? error : new Error(message || `请求失败：${url}`);
}

async function requestJson<T>(path: string, init?: RequestInit): Promise<T> {
  const headers: Record<string, string> = {
    "Content-Type": "application/json",
  };
  if (API_TOKEN) {
    headers["X-API-Token"] = API_TOKEN;
  }
  // 附加 Bearer 会话令牌
  const authToken = getAuthToken();
  if (authToken) {
    headers["Authorization"] = `Bearer ${authToken}`;
  }
  const url = buildApiUrl(path);
  let response: Response;
  try {
    response = await fetch(url, {
      ...init,
      headers: {
        ...headers,
        ...(init?.headers as Record<string, string> | undefined),
      },
    });
  } catch (error) {
    throw normalizeNetworkError(error, url);
  }
  const payload = await response.json().catch(() => ({}));
  if (!response.ok) {
    const detail = typeof payload?.detail === "string" ? payload.detail : `HTTP ${response.status}`;
    throw new Error(detail);
  }
  return payload as T;
}

async function requestFile(path: string, init?: RequestInit): Promise<DownloadedFilePayload> {
  const headers: Record<string, string> = {};
  if (API_TOKEN) {
    headers["X-API-Token"] = API_TOKEN;
  }
  const authToken = getAuthToken();
  if (authToken) {
    headers["Authorization"] = `Bearer ${authToken}`;
  }
  const url = buildApiUrl(path);
  let response: Response;
  try {
    response = await fetch(url, {
      ...init,
      headers: {
        ...headers,
        ...(init?.headers as Record<string, string> | undefined),
      },
    });
  } catch (error) {
    throw normalizeNetworkError(error, url);
  }
  if (!response.ok) {
    const payload = await response.json().catch(() => ({}));
    const detail = typeof payload?.detail === "string" ? payload.detail : `HTTP ${response.status}`;
    throw new Error(detail);
  }
  const blob = await response.blob();
  const disposition = response.headers.get("Content-Disposition") ?? "";
  const matched = disposition.match(/filename=\"?([^"]+)\"?/i);
  return {
    blob,
    filename: matched?.[1] ?? "download.bin",
    contentType: response.headers.get("Content-Type") ?? "application/octet-stream",
  };
}

export async function fetchBackendHealth(signal?: AbortSignal): Promise<Record<string, unknown>> {
  return requestJson<Record<string, unknown>>("/health", {
    method: "GET",
    signal,
  });
}

export async function fetchSystemStateStatus(signal?: AbortSignal): Promise<SystemStatePayload> {
  return requestJson<SystemStatePayload>("/api/v1/system/state-status", {
    method: "GET",
    signal,
  });
}

export async function fetchSystemFrontendEvents(signal?: AbortSignal): Promise<SystemFrontendEventsPayload> {
  return requestJson<SystemFrontendEventsPayload>("/api/v1/system/frontend-events?limit=8", {
    method: "GET",
    signal,
  });
}

export interface StructureQueryParams {
  code: string;
  interval: string;
  status: string;
  signalType: string;
  groupStrategy: string;
  minObservations: number;
  limit?: number;
  offset?: number;
}

function buildStructureQuery(params: StructureQueryParams): string {
  const query = new URLSearchParams();
  if (params.code) {
    query.set("code", params.code);
  }
  if (params.interval) {
    query.set("interval", params.interval);
  }
  if (params.status) {
    query.set("status", params.status);
  }
  query.set("include_bayes_meta", "1");
  query.set("group_strategy", params.groupStrategy);
  query.set("min_observations", String(params.minObservations));
  query.set("limit", String(params.limit ?? 100));
  query.set("offset", String(params.offset ?? 0));
  return query.toString();
}

export async function fetchStructures(
  params: StructureQueryParams,
  signal?: AbortSignal,
): Promise<StructuresPayload> {
  return requestJson<StructuresPayload>(`/api/v1/structures/?${buildStructureQuery(params)}`, {
    method: "GET",
    signal,
  });
}

export async function fetchStructuredSignals(
  params: StructureQueryParams,
  signal?: AbortSignal,
): Promise<StructuredSignalsPayload> {
  const query = new URLSearchParams();
  if (params.code) {
    query.set("code", params.code);
  }
  if (params.interval) {
    query.set("interval", params.interval);
  }
  if (params.signalType) {
    query.set("signal_type", params.signalType);
  }
  query.set("limit", String(params.limit ?? 100));
  query.set("offset", String(params.offset ?? 0));
  return requestJson<StructuredSignalsPayload>(`/api/v1/signals/?${query.toString()}`, {
    method: "GET",
    signal,
  });
}

export async function fetchStructureBayesianSummary(
  params: StructureQueryParams,
  signal?: AbortSignal,
): Promise<BayesianSummaryPayload> {
  const query = new URLSearchParams();
  if (params.code) {
    query.set("code", params.code);
  }
  if (params.interval) {
    query.set("interval", params.interval);
  }
  if (params.status) {
    query.append("status", params.status);
  }
  if (params.signalType) {
    query.append("signal_type", params.signalType);
  }
  query.append("group_by", "code");
  query.append("group_by", "interval");
  query.append("group_by", "direction");
  query.set("group_strategy", params.groupStrategy);
  query.set("min_observations", String(params.minObservations));
  return requestJson<BayesianSummaryPayload>(`/api/v1/structures/bayesian-baseline/summary?${query.toString()}`, {
    method: "GET",
    signal,
  });
}

export async function applyStructureBayesianBaseline(
  params: StructureQueryParams,
): Promise<ApplyBayesianPayload> {
  const query = new URLSearchParams();
  if (params.code) {
    query.set("code", params.code);
  }
  if (params.interval) {
    query.set("interval", params.interval);
  }
  if (params.status) {
    query.append("status", params.status);
  }
  if (params.signalType) {
    query.append("signal_type", params.signalType);
  }
  query.append("group_by", "code");
  query.append("group_by", "interval");
  query.append("group_by", "direction");
  query.set("group_strategy", params.groupStrategy);
  query.set("min_observations", String(params.minObservations));
  return requestJson<ApplyBayesianPayload>(`/api/v1/structures/bayesian-baseline/apply?${query.toString()}`, {
    method: "POST",
  });
}

export async function fetchStructureDetail(
  structureId: string,
  groupStrategy: string,
  minObservations: number,
  signal?: AbortSignal,
): Promise<StructureDetailPayload> {
  const query = new URLSearchParams();
  query.set("audit_limit", "12");
  query.set("include_bayes_meta", "1");
  query.set("group_strategy", groupStrategy);
  query.set("min_observations", String(minObservations));
  return requestJson<StructureDetailPayload>(`/api/v1/structures/${encodeURIComponent(structureId)}/detail?${query.toString()}`, {
    method: "GET",
    signal,
  });
}

export async function fetchDataGovernanceOverview(
  trendDays = 7,
  signal?: AbortSignal,
): Promise<DataGovernanceOverviewPayload> {
  const query = new URLSearchParams();
  query.set("trend_days", String(trendDays));
  return requestJson<DataGovernanceOverviewPayload>(`/api/v1/data-governance/overview?${query.toString()}`, {
    method: "GET",
    signal,
  });
}

export async function fetchDataGovernanceTradingCalendar(
  startDate: string,
  endDate: string,
  signal?: AbortSignal,
): Promise<DataGovernanceTradingCalendarPayload> {
  const query = new URLSearchParams();
  query.set("start_date", startDate);
  query.set("end_date", endDate);
  return requestJson<DataGovernanceTradingCalendarPayload>(`/api/v1/data-governance/trading-calendar?${query.toString()}`, {
    method: "GET",
    signal,
  });
}

export async function fetchDataGovernanceTraceability(
  stockCode: string,
  period: string,
  signal?: AbortSignal,
): Promise<DataGovernanceTraceabilityPayload> {
  const query = new URLSearchParams();
  if (stockCode) {
    query.set("stock_code", stockCode);
  }
  if (period) {
    query.set("period", period);
  }
  query.set("limit", "120");
  return requestJson<DataGovernanceTraceabilityPayload>(`/api/v1/data-governance/traceability?${query.toString()}`, {
    method: "GET",
    signal,
  });
}

export async function createDataIngestionJob(
  payload: DataIngestionJobCreateInput,
  signal?: AbortSignal,
): Promise<DataIngestionJobPayload> {
  return requestJson<DataIngestionJobPayload>("/api/v1/data-ingestion/jobs", {
    method: "POST",
    signal,
    headers: {
      "Content-Type": "application/json",
    },
    body: JSON.stringify({
      stock_codes: payload.stockCodes,
      use_all_stocks: payload.useAllStocks ?? false,
      exchanges: payload.exchanges,
      periods: payload.periods,
      start_date: payload.startDate,
      end_date: payload.endDate,
      precompute_after_download: payload.precomputeAfterDownload ?? true,
      enable_audit_after_ingest: payload.enableAuditAfterIngest ?? true,
      download_workers: payload.downloadWorkers,
      intraday_lookback_days: payload.intradayLookbackDays,
      stage_label: payload.stageLabel,
    }),
  });
}

export async function fetchDataIngestionJobs(
  params: { statusFilter?: DataIngestionJobStatus | ""; limit?: number } = {},
  signal?: AbortSignal,
): Promise<DataIngestionJobListPayload> {
  const query = new URLSearchParams();
  if (params.statusFilter) {
    query.set("status_filter", params.statusFilter);
  }
  query.set("limit", String(params.limit ?? 20));
  return requestJson<DataIngestionJobListPayload>(`/api/v1/data-ingestion/jobs?${query.toString()}`, {
    method: "GET",
    signal,
  });
}

export async function fetchDataIngestionJob(
  jobId: string,
  signal?: AbortSignal,
): Promise<DataIngestionJobPayload> {
  return requestJson<DataIngestionJobPayload>(`/api/v1/data-ingestion/jobs/${encodeURIComponent(jobId)}`, {
    method: "GET",
    signal,
  });
}

export async function cancelDataIngestionJob(
  jobId: string,
  signal?: AbortSignal,
): Promise<DataIngestionJobPayload> {
  return requestJson<DataIngestionJobPayload>(`/api/v1/data-ingestion/jobs/${encodeURIComponent(jobId)}`, {
    method: "DELETE",
    signal,
  });
}

export async function forceAbortDataIngestionJob(
  jobId: string,
  signal?: AbortSignal,
): Promise<DataIngestionJobPayload> {
  return requestJson<DataIngestionJobPayload>(`/api/v1/data-ingestion/jobs/${encodeURIComponent(jobId)}/force-abort`, {
    method: "POST",
    signal,
  });
}

export async function retryDataIngestionJob(
  jobId: string,
  signal?: AbortSignal,
): Promise<DataIngestionJobPayload> {
  return requestJson<DataIngestionJobPayload>(`/api/v1/data-ingestion/jobs/${encodeURIComponent(jobId)}/retry`, {
    method: "POST",
    signal,
  });
}

export async function fetchCoverageMatrixOverview(
  signal?: AbortSignal,
): Promise<CoverageMatrixOverviewPayload> {
  return requestJson<CoverageMatrixOverviewPayload>("/api/v1/data-quality/coverage-matrix", {
    method: "GET",
    signal,
  });
}

export async function createCoverageMatrixJob(
  payload: CoverageMatrixJobCreateInput = {},
  signal?: AbortSignal,
): Promise<CoverageMatrixJobPayload> {
  return requestJson<CoverageMatrixJobPayload>("/api/v1/data-quality/coverage-matrix/jobs", {
    method: "POST",
    signal,
    headers: {
      "Content-Type": "application/json",
    },
    body: JSON.stringify({
      config_path: payload.configPath ?? "",
      output_dir: payload.outputDir ?? "",
      workers: payload.workers,
      resume: payload.resume,
    }),
  });
}

export async function fetchCoverageMatrixJobs(
  params: { statusFilter?: CoverageMatrixJobStatus | ""; limit?: number } = {},
  signal?: AbortSignal,
): Promise<CoverageMatrixJobListPayload> {
  const query = new URLSearchParams();
  if (params.statusFilter) {
    query.set("status_filter", params.statusFilter);
  }
  query.set("limit", String(params.limit ?? 20));
  return requestJson<CoverageMatrixJobListPayload>(`/api/v1/data-quality/coverage-matrix/jobs?${query.toString()}`, {
    method: "GET",
    signal,
  });
}

export async function fetchCoverageMatrixJob(
  jobId: string,
  signal?: AbortSignal,
): Promise<CoverageMatrixJobPayload> {
  return requestJson<CoverageMatrixJobPayload>(`/api/v1/data-quality/coverage-matrix/jobs/${encodeURIComponent(jobId)}`, {
    method: "GET",
    signal,
  });
}

export async function cancelCoverageMatrixJob(
  jobId: string,
  signal?: AbortSignal,
): Promise<CoverageMatrixJobPayload> {
  return requestJson<CoverageMatrixJobPayload>(`/api/v1/data-quality/coverage-matrix/jobs/${encodeURIComponent(jobId)}`, {
    method: "DELETE",
    signal,
  });
}

export async function downloadCoverageMatrixArtifact(
  jobId: string,
  artifactKey: CoverageMatrixArtifactKey,
  signal?: AbortSignal,
): Promise<DownloadedFilePayload> {
  return requestFile(`/api/v1/data-quality/coverage-matrix/jobs/${encodeURIComponent(jobId)}/artifacts/${encodeURIComponent(artifactKey)}`, {
    method: "GET",
    signal,
  });
}

export async function fetchChartBars(
  params: ChartBarsQueryParams,
  signal?: AbortSignal,
): Promise<ChartBarsPayload> {
  const query = new URLSearchParams();
  query.set("symbol", params.symbol);
  query.set("interval", params.interval);
  // datetime-precision cursor 优先（分钟/小时线翻页），回退到 date-only（日/周线）
  if (params.startDatetime) {
    query.set("start_datetime", params.startDatetime);
  } else if (params.startDate) {
    query.set("start_date", params.startDate);
  }
  if (params.endDatetime) {
    query.set("end_datetime", params.endDatetime);
  } else if (params.endDate) {
    query.set("end_date", params.endDate);
  }
  if (params.adjust) {
    query.set("adjust", params.adjust);
  }
  query.set("limit", String(params.limit ?? 800));

  return requestJson<ChartBarsPayload>(`/api/v1/chart/bars?${query.toString()}`, {
    method: "GET",
    signal,
  });
}

export async function fetchChartPeriodCatalog(
  uiVisibleOnly = false,
  signal?: AbortSignal,
): Promise<ChartPeriodCatalogPayload> {
  const query = new URLSearchParams();
  if (uiVisibleOnly) {
    query.set("ui_visible_only", "true");
  }
  const suffix = query.size > 0 ? `?${query.toString()}` : "";
  return requestJson<ChartPeriodCatalogPayload>(`/api/v1/chart/periods${suffix}`, {
    method: "GET",
    signal,
  });
}

export async function fetchIngestionGateStatus(
  symbol: string,
  period: string,
  signal?: AbortSignal,
): Promise<IngestionGateStatusPayload> {
  const query = new URLSearchParams();
  query.set("symbol", symbol);
  query.set("period", period);
  return requestJson<IngestionGateStatusPayload>(`/api/v1/data-quality/ingestion-status?${query.toString()}`, {
    method: "GET",
    signal,
  });
}

export async function fetchMarketBarSubscriptions(
  signal?: AbortSignal,
): Promise<MarketBarSubscriptionsResponsePayload> {
  return requestJson<MarketBarSubscriptionsResponsePayload>("/api/v1/market/bar-subscriptions", {
    method: "GET",
    signal,
  });
}

export async function fetchGolden1dRepairPlan(
  symbol: string,
  signal?: AbortSignal,
): Promise<Golden1dRepairPlanPayload> {
  const query = new URLSearchParams();
  query.set("symbol", symbol);
  return requestJson<Golden1dRepairPlanPayload>(`/api/v1/data-quality/golden-1d-repair-plan?${query.toString()}`, {
    method: "GET",
    signal,
  });
}

export async function fetchGolden1dRepairPlans(
  limit = 12,
  signal?: AbortSignal,
): Promise<Golden1dRepairPlanListPayload> {
  const query = new URLSearchParams();
  query.set("limit", String(limit));
  return requestJson<Golden1dRepairPlanListPayload>(`/api/v1/data-quality/golden-1d-repair-plan?${query.toString()}`, {
    method: "GET",
    signal,
  });
}

export async function fetchReceiptHistory(
  receiptType: "publish_gate" | "repair" | "replay",
  limit = 20,
  signal?: AbortSignal,
): Promise<ReceiptHistoryPayload> {
  const query = new URLSearchParams();
  query.set("receipt_type", receiptType);
  query.set("limit", String(limit));
  return requestJson<ReceiptHistoryPayload>(`/api/v1/data-quality/receipts?${query.toString()}`, {
    method: "GET",
    signal,
  });
}

export async function fetchReceiptTimeline(
  params: {
    symbol?: string;
    period?: string;
    lineageAnchor?: string;
    receiptType?: "publish_gate" | "repair" | "replay";
    gateRejectReason?: string;
    severity?: "ok" | "warning" | "critical" | "unknown";
    lookbackDays?: number;
    limit?: number;
  } = {},
  signal?: AbortSignal,
): Promise<ReceiptTimelinePayload> {
  const query = new URLSearchParams();
  if (params.symbol) {
    query.set("symbol", params.symbol);
  }
  if (params.period) {
    query.set("period", params.period);
  }
  if (params.lineageAnchor) {
    query.set("lineage_anchor", params.lineageAnchor);
  }
  if (params.receiptType) {
    query.set("receipt_type", params.receiptType);
  }
  if (params.gateRejectReason) {
    query.set("gate_reject_reason", params.gateRejectReason);
  }
  if (params.severity) {
    query.set("severity", params.severity);
  }
  if (params.lookbackDays !== undefined) {
    query.set("lookback_days", String(params.lookbackDays));
  }
  query.set("limit", String(params.limit ?? 50));
  return requestJson<ReceiptTimelinePayload>(`/api/v1/data-quality/receipt-timeline?${query.toString()}`, {
    method: "GET",
    signal,
  });
}

export async function fetchLineageAnchorDetail(
  lineageAnchor: string,
  signal?: AbortSignal,
): Promise<LineageAnchorDetailPayload> {
  const query = new URLSearchParams();
  query.set("lineage_anchor", lineageAnchor);
  return requestJson<LineageAnchorDetailPayload>(`/api/v1/data-quality/lineage-anchor-detail?${query.toString()}`, {
    method: "GET",
    signal,
  });
}

export async function fetchGovernanceSlaThresholds(
  signal?: AbortSignal,
): Promise<GovernanceSlaThresholdPayload> {
  return requestJson<GovernanceSlaThresholdPayload>("/api/v1/data-governance/sla-thresholds", {
    method: "GET",
    signal,
  });
}

export async function updateGovernanceSlaThresholds(
  overrides: Record<string, number>,
  operator = "tauri-user",
  note = "",
  signal?: AbortSignal,
): Promise<GovernanceSlaThresholdPayload> {
  return requestJson<GovernanceSlaThresholdPayload>("/api/v1/data-governance/sla-thresholds", {
    method: "PATCH",
    signal,
    headers: {
      "Content-Type": "application/json",
    },
    body: JSON.stringify({ overrides, operator, note }),
  });
}

export async function fetchGovernanceActionAudit(
  params: { limit?: number; actionType?: string; source?: string; stockCode?: string; period?: string; lineageAnchor?: string } = {},
  signal?: AbortSignal,
): Promise<GovernanceActionAuditPayload> {
  const query = new URLSearchParams();
  query.set("limit", String(params.limit ?? 20));
  if (params.actionType) {
    query.set("action_type", params.actionType);
  }
  if (params.source) {
    query.set("source", params.source);
  }
  if (params.stockCode) {
    query.set("stock_code", params.stockCode);
  }
  if (params.period) {
    query.set("period", params.period);
  }
  if (params.lineageAnchor) {
    query.set("lineage_anchor", params.lineageAnchor);
  }
  return requestJson<GovernanceActionAuditPayload>(`/api/v1/data-governance/action-audit?${query.toString()}`, {
    method: "GET",
    signal,
  });
}

export async function createGovernanceActionAudit(
  payload: {
    actionId: string;
    actionType: string;
    tone?: string;
    title?: string;
    detail?: string;
    source?: string;
    payload?: Record<string, unknown>;
  },
  signal?: AbortSignal,
): Promise<GovernanceActionAuditPayload> {
  return requestJson<GovernanceActionAuditPayload>("/api/v1/data-governance/action-audit", {
    method: "POST",
    signal,
    headers: {
      "Content-Type": "application/json",
    },
    body: JSON.stringify({
      action_id: payload.actionId,
      action_type: payload.actionType,
      tone: payload.tone ?? "neutral",
      title: payload.title ?? "",
      detail: payload.detail ?? "",
      source: payload.source ?? "tauri-data-route",
      payload: payload.payload ?? {},
    }),
  });
}

export async function exportGovernanceSnapshot(
  trendDays = 7,
  auditLimit = 50,
  exportFormat: "json" | "jsonl" | "csv" = "json",
  signal?: AbortSignal,
): Promise<GovernanceSnapshotExportPayload | DownloadedFilePayload> {
  const query = new URLSearchParams();
  query.set("trend_days", String(trendDays));
  query.set("audit_limit", String(auditLimit));
  query.set("export_format", exportFormat);
  if (exportFormat === "json") {
    return requestJson<GovernanceSnapshotExportPayload>(`/api/v1/data-governance/export-snapshot?${query.toString()}`, {
      method: "GET",
      signal,
    });
  }
  return requestFile(`/api/v1/data-governance/export-snapshot?${query.toString()}`, {
    method: "GET",
    signal,
  });
}

export async function triggerGolden1dRepair(
  symbol: string,
  forceFull = false,
): Promise<Golden1dRepairTriggerPayload> {
  const query = new URLSearchParams();
  query.set("symbol", symbol);
  query.set("force_full", forceFull ? "true" : "false");
  return requestJson<Golden1dRepairTriggerPayload>(`/api/v1/data-quality/golden-1d-repair?${query.toString()}`, {
    method: "POST",
  });
}

export async function fetchGolden1dBatchStatus(
  signal?: AbortSignal,
): Promise<Golden1dBatchStatusPayload> {
  return requestJson<Golden1dBatchStatusPayload>("/api/v1/data-quality/golden-1d-status", {
    method: "GET",
    signal,
  });
}

export async function triggerGolden1dBatchAudit(
  forceFull = false,
  limit = 50,
  signal?: AbortSignal,
): Promise<Golden1dBatchAuditResultPayload> {
  const query = new URLSearchParams();
  query.set("force_full", forceFull ? "true" : "false");
  query.set("limit", String(limit));
  return requestJson<Golden1dBatchAuditResultPayload>(`/api/v1/data-quality/golden-1d-audit?${query.toString()}`, {
    method: "POST",
    signal,
  });
}

export async function fetchPeriodAssetMatrix(
  signal?: AbortSignal,
): Promise<PeriodAssetMatrixPayload> {
  return requestJson<PeriodAssetMatrixPayload>(`/api/v1/data-quality/period-asset-matrix`, { signal });
}

export async function fetchDbTablesOverview(
  params: { includeColumns?: boolean; includeEmpty?: boolean } = {},
  signal?: AbortSignal,
): Promise<DbTablesOverviewPayload> {
  const query = new URLSearchParams();
  query.set("include_columns", String(params.includeColumns ?? true));
  query.set("include_empty", String(params.includeEmpty ?? true));
  return requestJson<DbTablesOverviewPayload>(`/api/v1/db/tables?${query.toString()}`, {
    method: "GET",
    signal,
  });
}

export async function fetchDbTableRows(
  tableName: string,
  params: { limit?: number; offset?: number } = {},
  signal?: AbortSignal,
): Promise<DbTableRowsPayload> {
  const query = new URLSearchParams();
  query.set("limit", String(params.limit ?? 20));
  query.set("offset", String(params.offset ?? 0));
  return requestJson<DbTableRowsPayload>(`/api/v1/db/tables/${encodeURIComponent(tableName)}/rows?${query.toString()}`, {
    method: "GET",
    signal,
  });
}

export async function downloadDbTableExport(
  tableName: string,
  exportFormat: "csv" | "jsonl" = "csv",
  limit = 5000,
  signal?: AbortSignal,
): Promise<DownloadedFilePayload> {
  const query = new URLSearchParams();
  query.set("export_format", exportFormat);
  query.set("limit", String(limit));
  return requestFile(`/api/v1/db/tables/${encodeURIComponent(tableName)}/export?${query.toString()}`, {
    method: "GET",
    signal,
  });
}

export async function executeDbQuery(
  sql: string,
  limit = 200,
  signal?: AbortSignal,
): Promise<DbQueryResultPayload> {
  return requestJson<DbQueryResultPayload>("/api/v1/db/query", {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ sql, limit }),
    signal,
  });
}

export async function importDbCsv(
  tableName: string,
  csvContent: string,
  mode: "append" | "replace" = "append",
  signal?: AbortSignal,
): Promise<DbCsvImportResultPayload> {
  return requestJson<DbCsvImportResultPayload>("/api/v1/db/import-csv", {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ table_name: tableName, csv_content: csvContent, mode }),
    signal,
  });
}

export interface IntegrityCheckPayload {
  stock_code: string;
  check_range: [string, string];
  missing_trading_days: number;
  completeness_ratio: number;
  quality_report: {
    errors: number;
    warnings: number;
    info: number;
    issues: string[];
    warning_messages: string[];
  };
  status: "PASS" | "FAIL";
  elapsed_ms: number;
  server_time: number;
  build_version?: string;
  commit_sha?: string;
}

export interface ReconciliationPayload {
  stock_code: string;
  start_date: string;
  end_date: string;
  duckdb_rows: number;
  akshare_rows: number;
  compared_rows: number;
  consistent: boolean;
  consistency_rate: number;
  max_diff_pct: number;
  diff_days: string[];
  source: string;
  error?: string;
  server_time: number;
  build_version?: string;
  commit_sha?: string;
}

export async function runIntegrityCheck(
  stockCode: string,
  startDate: string,
  endDate: string,
  detailed = true,
  signal?: AbortSignal,
): Promise<IntegrityCheckPayload> {
  return requestJson<IntegrityCheckPayload>("/api/v1/data-quality/integrity-check", {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ stock_code: stockCode, start_date: startDate, end_date: endDate, detailed }),
    signal,
  });
}

export async function runReconciliation(
  stockCode: string,
  startDate: string,
  endDate: string,
  signal?: AbortSignal,
): Promise<ReconciliationPayload> {
  return requestJson<ReconciliationPayload>("/api/v1/data-quality/reconciliation", {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ stock_code: stockCode, start_date: startDate, end_date: endDate }),
    signal,
  });
}

// ---------------------------------------------------------------------------
// B4: DuckDB 维护 + 数据源配置
// ---------------------------------------------------------------------------

export interface DbMaintenancePayload {
  operation: string;
  success: boolean;
  message: string;
  table_stats?: Array<{ table: string; rows: number | null }>;
  elapsed_ms: number;
  server_time: number;
  build_version?: string;
  commit_sha?: string;
}

export type DataSourceKey =
  | "qmt_local_dat"
  | "qmt_xtquant"
  | "qmt"
  | "duckdb"
  | "tushare"
  | "akshare"
  | "baostock"
  | "mootdx"
  | "pytdx_local_tdx"
  | "qstock"
  | "tqsdk";

export interface DataSourceDiscoveryCandidatePayload {
  install_path?: string | null;
  userdata_path?: string | null;
  datadir_path?: string | null;
  cache_root?: string | null;
  discovered_by?: string;
  has_userdata?: boolean;
  has_datadir?: boolean;
  has_downloaded_history?: boolean;
  has_local_cache?: boolean;
  market_dirs?: string[];
  period_dirs?: string[];
  dat_file_count_hint?: number;
  history_sample_files?: string[];
  latest_modified_at?: string | null;
  score?: number;
}

export interface DataSourceDiscoveryScanPolicyPayload {
  mode: string;
  max_depth: number;
  env_override: boolean;
  configured_roots: string[];
  default_roots: string[];
  effective_roots: string[];
  effective_roots_count: number;
}

export interface DataSourceDiscoveryScanMetricsPayload {
  started_at: string | null;
  finished_at: string | null;
  duration_ms: number | null;
  roots_scanned: number;
  roots_with_matches: number;
  roots_with_errors: number;
}

export interface DataSourceDiscoveryRootResultPayload {
  root: string | null;
  status: string;
  match_events: number;
  error_count: number;
  errors: string[];
  error_summary?: Record<string, number>;
}

export interface DataSourceDiscoveryPayload {
  search_roots: string[];
  scan_policy?: DataSourceDiscoveryScanPolicyPayload;
  scan_metrics?: DataSourceDiscoveryScanMetricsPayload;
  root_results?: DataSourceDiscoveryRootResultPayload[];
  candidate_count: number;
  candidates: DataSourceDiscoveryCandidatePayload[];
}

export interface DataSourceItem {
  source: DataSourceKey | string;
  label: string;
  configured: boolean;
  token_masked?: string | null;
  credential_masked?: string | null;
  password_configured?: boolean;
  path?: string | null;
  path_exists?: boolean;
  note: string;
  priority?: number;
  category?: string;
  anchor_kind?: string;
  capabilities?: string[];
  discovery_key?: string | null;
  testable?: boolean;
}

export interface DataSourceConfigPayload {
  sources: DataSourceItem[];
  discovery?: Record<string, DataSourceDiscoveryPayload>;
  server_time: number;
  build_version?: string;
  commit_sha?: string;
  cache_status?: "fresh" | "miss" | "stale";
  last_scan_at?: string;
  cache_age_ms?: number;
}

export interface DataSourceTestPayload {
  source: DataSourceKey | string;
  status: "ok" | "error" | "unconfigured";
  message: string;
  latency_ms: number | null;
  server_time: number;
  detail?: unknown;
  path?: string;
}

export async function runDbMaintenance(
  operation: "checkpoint" | "force_checkpoint" | "analyze",
  signal?: AbortSignal,
): Promise<DbMaintenancePayload> {
  return requestJson<DbMaintenancePayload>("/api/v1/db/maintenance", {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ operation }),
    signal,
  });
}

export async function fetchDataSourceConfig(signal?: AbortSignal): Promise<DataSourceConfigPayload> {
  return requestJson<DataSourceConfigPayload>("/api/v1/datasource/config", { signal });
}

export async function triggerDatasourceRescan(signal?: AbortSignal): Promise<DataSourceConfigPayload> {
  return requestJson<DataSourceConfigPayload>("/api/v1/datasource/rescan", {
    method: "POST",
    signal,
  });
}

export interface SourcePriorityPayload {
  priorities: Record<string, number>;
}

export interface SourcePriorityPatchResponse {
  priorities: Record<string, number>;
  sources: DataSourceItem[];
}

export async function fetchSourcePriority(signal?: AbortSignal): Promise<SourcePriorityPayload> {
  return requestJson<SourcePriorityPayload>("/api/v1/datasource/source-priority", { signal });
}

export async function patchSourcePriority(
  updates: Record<string, number>,
  signal?: AbortSignal,
): Promise<SourcePriorityPatchResponse> {
  return requestJson<SourcePriorityPatchResponse>("/api/v1/datasource/source-priority", {
    method: "PATCH",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ updates }),
    signal,
  });
}

// ---------------------------------------------------------------------------
// 数据源凭证管理（Tushare Token / TQSdk 账户密码）
// ---------------------------------------------------------------------------

export interface DatasourceCredentialsMasked {
  source: string;
  // tushare
  configured?: boolean;
  token_masked?: string | null;
  credential_masked?: string | null;
  // tqsdk
  username?: string | null;
  username_masked?: string | null;
  password_configured?: boolean;
}

export interface DatasourceCredentialsPatch {
  token?: string;
  username?: string;
  password?: string;
}

export async function fetchDatasourceCredentials(
  source: string,
  signal?: AbortSignal,
): Promise<DatasourceCredentialsMasked> {
  return requestJson<DatasourceCredentialsMasked>(`/api/v1/datasource/${source}/credentials`, { signal });
}

export async function patchDatasourceCredentials(
  source: string,
  payload: DatasourceCredentialsPatch,
  signal?: AbortSignal,
): Promise<DatasourceCredentialsMasked> {
  return requestJson<DatasourceCredentialsMasked>(`/api/v1/datasource/${source}/credentials`, {
    method: "PATCH",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(payload),
    signal,
  });
}

export async function deleteDatasourceCredentials(
  source: string,
  signal?: AbortSignal,
): Promise<{ source: string; cleared: boolean }> {
  return requestJson<{ source: string; cleared: boolean }>(
    `/api/v1/datasource/${source}/credentials`,
    { method: "DELETE", signal },
  );
}

export async function testDataSource(
  source: DataSourceKey,
  signal?: AbortSignal,
): Promise<DataSourceTestPayload> {
  return requestJson<DataSourceTestPayload>("/api/v1/datasource/test", {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ source }),
    signal,
  });
}

// ---------------------------------------------------------------------------
// QMT 全盘扫描
// ---------------------------------------------------------------------------

/** 单个 QMT 候选安装实例（来自 /api/v1/qmt/local-scan）。 */
export interface QmtLocalCandidate {
  install_path: string | null;
  userdata_path: string;
  datadir_path: string | null;
  discovered_by: string;
  has_userdata: boolean;
  has_datadir: boolean;
  has_downloaded_history: boolean;
  market_dirs: string[];
  period_dirs: string[];
  dat_file_count_hint: number;
  history_sample_files: string[];
  latest_modified_at: string | null;
  score: number;
}

/** 扫描策略元数据。 */
export interface QmtScanPolicy {
  mode: string;
  max_depth: number;
  env_override: boolean;
  configured_roots: string[];
  default_roots: string[];
  effective_roots: string[];
  effective_roots_count: number;
}

/** 根扫描结果（每个搜索根的按错误/命中统计）。 */
export interface QmtScanRootResult {
  root: string;
  status: string;
  match_events: number;
  error_count: number;
  errors: string[];
  error_summary: Record<string, number>;
}

/** 扫描耗时指标。 */
export interface QmtScanMetrics {
  started_at: string | null;
  finished_at: string | null;
  duration_ms: number | null;
  roots_scanned: number;
  roots_with_matches: number;
  roots_with_errors: number;
}

/** /api/v1/qmt/local-scan 完整响应。 */
export interface QmtLocalScanPayload {
  search_roots: string[];
  scan_policy: QmtScanPolicy;
  scan_metrics: QmtScanMetrics;
  root_results: QmtScanRootResult[];
  candidate_count: number;
  candidates: QmtLocalCandidate[];
  scan_duration_ms: number;
  server_time: number;
}

export interface QmtChannelProfilePayload {
  channel_id: string;
  broker_id: string;
  broker_guess: string;
  channel_kind: string;
  cfg_fingerprint?: string;
  supported_account_types: string[];
  server_endpoint_hint?: string;
  login_entry_hint?: string;
}

export interface QmtLayoutItemPayload {
  layout_id: string;
  fingerprint: string;
  install_root: string;
  exe_path: string;
  bin_path: string;
  userdata_roots: string[];
  datadir_paths: string[];
  data_paths: string[];
  datas_paths: string[];
  cfg_paths: string[];
  xtquant_roots: string[];
  discovered_from: string[];
  scan_root: string;
  confidence_score: number;
  observed_at?: string;
  status: string;
}

export interface QmtAssetItemPayload {
  asset_id: string;
  layout_id: string;
  userdata_path: string;
  datadir_path: string;
  data_path: string;
  datas_path: string;
  cfg_path: string;
  market_coverage: string[];
  period_coverage: string[];
  instrument_family_coverage: string[];
  latest_modified_at: string;
  readable_sample_rate: number;
  integrity_flags: string[];
  tick_score: number;
  m1_score: number;
  m5_score: number;
  d1_score: number;
  stability_score: number;
  freshness_state?: string;
  confidence_score: number;
  status: string;
}

export interface QmtProbeItemPayload {
  probe_id: string;
  layout_id: string;
  channel_id: string;
  userdata_path: string;
  broker_id: string;
  account_id: string;
  account_type: string;
  login_status: string;
  reachable: boolean;
  probe_method: string;
  probe_success: boolean;
  probe_error_code?: string;
  probe_error_message?: string;
  freshness_state?: string;
  observed_at?: string;
  status: string;
}

export interface QmtSessionItemPayload {
  session_id: string;
  session_anchor_key: string;
  layout_id: string;
  userdata_path: string;
  connected_accounts: string[];
  current_route_claims: string[];
  channel_profile: QmtChannelProfilePayload;
  process_status: string;
  login_status: string;
  session_health: string;
  connected: boolean;
  authenticated: boolean;
  last_error: string;
  freshness_state?: string;
  observed_at?: string;
  status: string;
}

export interface QmtConflictItemPayload {
  code: string;
  severity: string;
  message: string;
  target_kind: string;
  target_id: string;
  observed_at: string;
  details: Record<string, unknown>;
}

export interface QmtRouteDecisionItemPayload {
  snapshot_id: string;
  policy_id: string;
  purpose: string;
  account_id: string;
  candidate_ids: string[];
  winner: string;
  runner_up: string;
  score_breakdown: Record<string, number>;
  decision_reason: string;
  rejection_reasons: string[];
  effective_from?: string;
}

export interface QmtLayoutsPayload {
  items: QmtLayoutItemPayload[];
  total: number;
  cache_status?: "fresh" | "miss" | "stale";
  cache_age_ms?: number;
  last_scan_at?: string;
  candidate_count?: number;
  server_time: number;
}

export interface QmtAssetsPayload {
  items: QmtAssetItemPayload[];
  total: number;
  cache_status?: "fresh" | "miss" | "stale";
  cache_age_ms?: number;
  last_scan_at?: string;
  candidate_count?: number;
  server_time: number;
}

export interface QmtProbesPayload {
  items: QmtProbeItemPayload[];
  total: number;
  errors: string[];
  server_time: number;
}

export interface QmtSessionsPayload {
  items: QmtSessionItemPayload[];
  total: number;
  cache_size?: number;
  server_time: number;
}

export interface QmtConflictsPayload {
  items: QmtConflictItemPayload[];
  total: number;
  include_probes: boolean;
  probe_errors: string[];
  server_time: number;
}

export interface QmtRouteDecisionsPayload {
  items: QmtRouteDecisionItemPayload[];
  total: number;
  server_time: number;
}

export interface AccountBindingRecommendationPayload {
  accountId?: string;
  status: "matched" | "suggested" | "missing";
  message: string;
  candidatePath: string | null;
}

export interface AccountBindingItemPayload {
  binding_id: string;
  broker_account_id: string;
  probe_id: string;
  channel_id: string;
  asset_id: string;
  session_anchor_key: string;
  binding_scope: string;
  priority: number;
  manual_override: boolean;
  sticky_until: string;
  intent_source: string;
  change_reason: string;
  updated_by: string;
  disabled_reason: string;
  conflict_flags: string[];
  approval_required: boolean;
  approval_state: string;
  schema_version: number;
  entity_version: number;
  created_at: string;
  updated_at: string;
  source_refs: string[];
  evidence_ts: string;
  collector_version: string;
  confidence_score: number;
  status: string;
  notes: string;
  account_label: string;
  broker: string;
  trade_account: string;
  configured_userdata_path: string;
  configured_exe_path: string;
  recommendation_status: AccountBindingRecommendationPayload["status"];
  recommendation_message: string;
  candidate_path: string | null;
  apply_path: string | null;
  reasons: string[];
  session: QmtSessionItemPayload | null;
  probe: QmtProbeItemPayload | null;
  route: QmtRouteDecisionItemPayload | null;
  conflicts: QmtConflictItemPayload[];
}

export interface AccountBindingsPayload {
  items: AccountBindingItemPayload[];
  total: number;
  include_probes: boolean;
  probe_errors: string[];
  server_time: number;
}

export interface AccountBindingApplyResultPayload {
  operation: "apply";
  binding_id: string;
  broker_account_id: string;
  applied_path: string;
  updated: boolean;
  include_probes: boolean;
  force: boolean;
  binding: AccountBindingItemPayload | null;
  account: BrokerAccountPayload | null;
  server_time: number;
}

export async function fetchQmtLocalScan(opts?: {
  searchRoots?: string;
  limit?: number;
  force?: boolean;
  signal?: AbortSignal;
}): Promise<QmtLocalScanPayload> {
  const params = new URLSearchParams();
  if (opts?.searchRoots) params.set("search_roots", opts.searchRoots);
  if (opts?.limit != null) params.set("limit", String(opts.limit));
  if (opts?.force) params.set("force", "true");
  const qs = params.toString();
  return requestJson<QmtLocalScanPayload>(`/api/v1/qmt/local-scan${qs ? `?${qs}` : ""}`, {
    signal: opts?.signal,
  });
}

export async function fetchQmtLayouts(
  opts: { force?: boolean; signal?: AbortSignal } = {},
): Promise<QmtLayoutsPayload> {
  const query = new URLSearchParams();
  if (opts.force) {
    query.set("force", "true");
  }
  const suffix = query.size > 0 ? `?${query.toString()}` : "";
  return requestJson<QmtLayoutsPayload>(`/api/v1/qmt/layouts${suffix}`, { signal: opts.signal });
}

export async function fetchQmtAssets(
  opts: { force?: boolean; signal?: AbortSignal } = {},
): Promise<QmtAssetsPayload> {
  const query = new URLSearchParams();
  if (opts.force) {
    query.set("force", "true");
  }
  const suffix = query.size > 0 ? `?${query.toString()}` : "";
  return requestJson<QmtAssetsPayload>(`/api/v1/qmt/assets${suffix}`, { signal: opts.signal });
}

export async function fetchQmtProbes(
  opts: { userdataPath?: string; signal?: AbortSignal } = {},
): Promise<QmtProbesPayload> {
  const query = new URLSearchParams();
  if (opts.userdataPath) {
    query.set("userdata_path", opts.userdataPath);
  }
  const suffix = query.size > 0 ? `?${query.toString()}` : "";
  return requestJson<QmtProbesPayload>(`/api/v1/qmt/probes${suffix}`, { signal: opts.signal });
}

export async function fetchQmtSessions(signal?: AbortSignal): Promise<QmtSessionsPayload> {
  return requestJson<QmtSessionsPayload>("/api/v1/qmt/sessions", { signal });
}

export async function fetchQmtConflicts(
  opts: { includeProbes?: boolean; force?: boolean; signal?: AbortSignal } = {},
): Promise<QmtConflictsPayload> {
  const query = new URLSearchParams();
  if (opts.includeProbes) {
    query.set("include_probes", "true");
  }
  if (opts.force) {
    query.set("force", "true");
  }
  const suffix = query.size > 0 ? `?${query.toString()}` : "";
  return requestJson<QmtConflictsPayload>(`/api/v1/qmt/conflicts${suffix}`, { signal: opts.signal });
}

export async function fetchQmtRouteDecisions(
  opts: { force?: boolean; signal?: AbortSignal } = {},
): Promise<QmtRouteDecisionsPayload> {
  const query = new URLSearchParams();
  if (opts.force) {
    query.set("force", "true");
  }
  const suffix = query.size > 0 ? `?${query.toString()}` : "";
  return requestJson<QmtRouteDecisionsPayload>(`/api/v1/qmt/route-decisions${suffix}`, { signal: opts.signal });
}

export async function fetchAccountBindings(
  opts: { includeProbes?: boolean; force?: boolean; signal?: AbortSignal } = {},
): Promise<AccountBindingsPayload> {
  const query = new URLSearchParams();
  if (opts.includeProbes) {
    query.set("include_probes", "true");
  }
  if (opts.force) {
    query.set("force", "true");
  }
  const suffix = query.size > 0 ? `?${query.toString()}` : "";
  return requestJson<AccountBindingsPayload>(`/api/v1/account-bindings${suffix}`, { signal: opts.signal });
}

export async function discoverAccountBindings(
  opts: { includeProbes?: boolean; force?: boolean; signal?: AbortSignal } = {},
): Promise<AccountBindingsPayload> {
  return requestJson<AccountBindingsPayload>("/api/v1/account-bindings/discover", {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({
      include_probes: Boolean(opts.includeProbes),
      force: Boolean(opts.force),
    }),
    signal: opts.signal,
  });
}

export async function applyAccountBinding(
  bindingId: string,
  opts: { includeProbes?: boolean; force?: boolean; signal?: AbortSignal } = {},
): Promise<AccountBindingApplyResultPayload> {
  return requestJson<AccountBindingApplyResultPayload>(`/api/v1/account-bindings/${encodeURIComponent(bindingId)}/apply`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({
      include_probes: opts.includeProbes ?? true,
      force: opts.force ?? true,
    }),
    signal: opts.signal,
  });
}

export interface QmtRuntimeConfigPayload {
  config_path: string;
  exists: boolean;
  qmt_path: string;
  qmt_userdata_path: string;
  last_updated: string | null;
  server_time?: number;
}

export interface QmtRuntimeConfigApplyResultPayload {
  operation: "sync_from_broker_account";
  broker_account_id: string;
  updated: boolean;
  synced_fields: string[];
  runtime_config: QmtRuntimeConfigPayload;
  account: BrokerAccountPayload | null;
  server_time: number;
}

export async function fetchQmtRuntimeConfig(
  signal?: AbortSignal,
): Promise<QmtRuntimeConfigPayload> {
  return requestJson<QmtRuntimeConfigPayload>("/api/v1/qmt/runtime-config", {
    method: "GET",
    signal,
  });
}

export async function applyQmtRuntimeConfigFromAccount(
  accountId: string,
  signal?: AbortSignal,
): Promise<QmtRuntimeConfigApplyResultPayload> {
  return requestJson<QmtRuntimeConfigApplyResultPayload>(
    `/api/v1/qmt/runtime-config/apply-account/${encodeURIComponent(accountId)}`,
    {
      method: "POST",
      signal,
    },
  );
}

// ---------------------------------------------------------------------------
// Interface registry
// ---------------------------------------------------------------------------

export type InterfaceClass =
  | "market_data"
  | "trade_gateway"
  | "account_broker"
  | "storage_backend"
  | "control_ops";

export type InterfaceProtocol =
  | "local_file"
  | "local_sdk"
  | "rest_api"
  | "ctp"
  | "xtp"
  | "mmap_shm"
  | "sql_db"
  | "sql_tsdb"
  | "websocket";

export type InterfaceStatusIndicator = "configured" | "unconfigured" | "planned";

export interface InterfaceProfilePayload {
  source: string;
  label: string;
  interface_class: InterfaceClass;
  protocol: InterfaceProtocol | string;
  status_indicator: InterfaceStatusIndicator;
  configured: boolean;
  note: string;
  priority: number;
  capabilities: string[];
  testable: boolean;
  // optional runtime extras
  latency_ms?: number | null;
  last_probe_at?: string | null;
  // inventory extras (market_data sources)
  path?: string | null;
  path_exists?: boolean;
  token_masked?: string | null;
  credential_masked?: string | null;
  password_configured?: boolean;
  category?: string;
  anchor_kind?: string;
}

export interface InterfaceRegistryPayload {
  interfaces: InterfaceProfilePayload[];
  summary: {
    total: number;
    configured: number;
    unconfigured: number;
    planned: number;
  };
  groups: Record<InterfaceClass | string, InterfaceProfilePayload[]>;
  server_time: number;
  cache_status?: "fresh" | "miss" | "stale";
  last_scan_at?: string;
  cache_age_ms?: number;
}

export async function fetchInterfaceRegistry(signal?: AbortSignal): Promise<InterfaceRegistryPayload> {
  return requestJson<InterfaceRegistryPayload>("/api/v1/interfaces", { signal });
}

export async function triggerLateEventReplay(
  symbol: string,
  period: string,
  maxRetries = 3,
  limit = 20,
  reasonRegex = "(late|out_of_order|watermark|stale|reorder)",
): Promise<LateEventReplayTriggerPayload> {
  const query = new URLSearchParams();
  if (symbol) {
    query.set("symbol", symbol);
  }
  if (period) {
    query.set("period", period);
  }
  query.set("max_retries", String(maxRetries));
  query.set("limit", String(limit));
  query.set("reason_regex", reasonRegex);
  return requestJson<LateEventReplayTriggerPayload>(`/api/v1/data-quality/late-event-replay?${query.toString()}`, {
    method: "POST",
  });
}

// ---------------------------------------------------------------------------
// 标的搜索
// ---------------------------------------------------------------------------

export type SymbolSearchScope = "all" | "stock" | "etf" | "index" | "bond" | "commodity" | "other";

export interface SymbolSearchItemPayload {
  symbol: string;
  short_code: string;
  name: string;
  display_name: string;
  exchange: string;
  scope: SymbolSearchScope;
  scope_label: string;
  market_label: string;
  product_name: string;
  last_date: string | null;
}

export interface SymbolSearchPayload {
  q: string;
  limit: number;
  scope: SymbolSearchScope;
  items: SymbolSearchItemPayload[];
  total: number;
}

export async function fetchSymbolSearch(
  q: string,
  options: { limit?: number; scope?: SymbolSearchScope; signal?: AbortSignal } = {},
): Promise<SymbolSearchPayload> {
  const query = new URLSearchParams({
    q,
    limit: String(options.limit ?? 30),
    scope: options.scope ?? "all",
  });
  return requestJson<SymbolSearchPayload>(`/api/v1/symbols/search?${query.toString()}`, {
    signal: options.signal,
  });
}

// ---------------------------------------------------------------------------
// 策略管理
// ---------------------------------------------------------------------------

export interface StrategyInfoPayload {
  strategy_id: string;
  account_id: string;
  status: "created" | "running" | "paused" | "stopped" | "error";
  tags: string[];
  params: Record<string, unknown>;
  registered_at: string | null;
  has_instance: boolean;
}

export async function fetchStrategiesList(
  signal?: AbortSignal,
  statusFilter = "",
): Promise<StrategyInfoPayload[]> {
  const query = new URLSearchParams();
  if (statusFilter) query.set("status_filter", statusFilter);
  const qs = query.toString();
  return requestJson<StrategyInfoPayload[]>(
    `/api/v1/strategies/${qs ? `?${qs}` : ""}`,
    { method: "GET", signal },
  );
}


// ---------------------------------------------------------------------------
// 用户认证
// ---------------------------------------------------------------------------

export interface AuthUser {
  user_id: string;
  display_name: string;
  role: string;
  permissions?: string[];
  last_login?: number;
}

export interface LoginResult {
  token: string;
  user_id: string;
  expires_at: number;
  role: string;
  display_name: string;
}

export async function authLogin(userId: string, password: string): Promise<LoginResult> {
  const result = await requestJson<LoginResult>("/api/v1/auth/login", {
    method: "POST",
    body: JSON.stringify({ user_id: userId, password }),
  });
  // 登录成功 → 持久化令牌
  setAuthToken(result.token);
  setStoredUser({
    user_id: result.user_id,
    display_name: result.display_name,
    role: result.role,
  });
  return result;
}

export async function authLogout(): Promise<void> {
  try {
    await requestJson<{ ok: boolean }>("/api/v1/auth/logout", { method: "POST" });
  } catch {
    // 忽略网络错误
  }
  clearAuthToken();
}

export async function authMe(signal?: AbortSignal): Promise<AuthUser> {
  return requestJson<AuthUser>("/api/v1/auth/me", { method: "GET", signal });
}

export async function authChangePassword(
  oldPassword: string,
  newPassword: string,
): Promise<{ ok: boolean; message: string }> {
  return requestJson("/api/v1/auth/change-password", {
    method: "POST",
    body: JSON.stringify({ old_password: oldPassword, new_password: newPassword }),
  });
}


// ---------------------------------------------------------------------------
// 用户管理 (admin)
// ---------------------------------------------------------------------------

export async function fetchUsersList(signal?: AbortSignal): Promise<AuthUser[]> {
  return requestJson<AuthUser[]>("/api/v1/users/", { method: "GET", signal });
}

export async function createUser(
  userId: string,
  password: string,
  displayName: string,
  role: string,
  permissions: string[] = [],
): Promise<{ user_id: string; role: string; created: boolean }> {
  return requestJson("/api/v1/users/", {
    method: "POST",
    body: JSON.stringify({
      user_id: userId,
      password,
      display_name: displayName,
      role,
      permissions,
    }),
  });
}

export async function deleteUser(userId: string): Promise<{ user_id: string; deleted: boolean }> {
  return requestJson(`/api/v1/users/${encodeURIComponent(userId)}`, { method: "DELETE" });
}


// ---------------------------------------------------------------------------
// 券商多账户管理
// ---------------------------------------------------------------------------

export interface BrokerAccountPayload {
  id: string;
  owner_user_id: string;
  label: string;
  broker: string;
  account_types: string[];
  qmt_exe_path: string;
  qmt_userdata_path: string;
  trade_account: string;
  display_account?: string;
  has_password: boolean;
  is_active: boolean;
  is_default: boolean;
  created_at: number;
  updated_at: number;
  last_connected: number;
  notes: string;
}

export async function fetchBrokerAccounts(
  signal?: AbortSignal,
): Promise<BrokerAccountPayload[]> {
  return requestJson<BrokerAccountPayload[]>("/api/v1/broker-accounts/", {
    method: "GET",
    signal,
  });
}

export async function addBrokerAccount(
  data: Partial<BrokerAccountPayload> & { trade_password?: string },
): Promise<BrokerAccountPayload> {
  return requestJson<BrokerAccountPayload>("/api/v1/broker-accounts/", {
    method: "POST",
    body: JSON.stringify(data),
  });
}

export async function getBrokerAccount(
  accountId: string,
  signal?: AbortSignal,
): Promise<BrokerAccountPayload> {
  return requestJson<BrokerAccountPayload>(
    `/api/v1/broker-accounts/${encodeURIComponent(accountId)}`,
    { method: "GET", signal },
  );
}

export async function updateBrokerAccount(
  accountId: string,
  data: Partial<BrokerAccountPayload> & { trade_password?: string },
): Promise<BrokerAccountPayload> {
  return requestJson<BrokerAccountPayload>(
    `/api/v1/broker-accounts/${encodeURIComponent(accountId)}`,
    { method: "PATCH", body: JSON.stringify(data) },
  );
}

export async function deleteBrokerAccount(
  accountId: string,
): Promise<{ id: string; deleted: boolean }> {
  return requestJson(`/api/v1/broker-accounts/${encodeURIComponent(accountId)}`, {
    method: "DELETE",
  });
}

export async function patchStrategyStatus(
  strategyId: string,
  newStatus: string,
): Promise<{ strategy_id: string; status: string; updated: boolean }> {
  return requestJson(`/api/v1/strategies/${encodeURIComponent(strategyId)}/status`, {
    method: "PATCH",
    body: JSON.stringify({ status: newStatus }),
  });
}

// ── 账户管理 ───────────────────────────────────────────────

export interface AccountInfoPayload {
  account_id: string;
  enabled: boolean;
  created_at_ms: number | null;
  updated_at_ms: number | null;
  [key: string]: unknown;
}

export async function fetchAccountsList(
  signal?: AbortSignal,
): Promise<AccountInfoPayload[]> {
  return requestJson<AccountInfoPayload[]>("/api/v1/accounts/", { signal });
}

// ── 交易执行 ───────────────────────────────────────────────

export interface TradingAccountAssetPayload {
  account_id: string;
  cash: number;
  frozen_cash: number;
  market_value: number;
  total_asset: number;
}

export interface TradingPositionPayload {
  code: string;
  volume: number;
  can_use_volume: number;
  open_price: number;
  avg_price: number;        // 成本价（加权均价）
  market_value: number;
  frozen_volume: number;
  on_road_volume: number;   // 在途股份
  yesterday_volume: number; // 昨夜拥股
}

export interface TradingPositionLifecyclePayload {
  position_id: string;
  account_id: string;
  code: string;
  direction: string;
  status: "open" | "closed" | string;
  execution_mode: string;
  strategy_id: string;
  strategy_run_id: string;
  matching_policy: string;
  entry_avg_price: number;
  entry_total_qty: number;
  entry_total_amount: number;
  entry_commission: number;
  first_entry_at: number;
  last_entry_at: number;
  peak_qty: number;
  remaining_qty: number;
  exit_avg_price: number;
  exit_total_qty: number;
  exit_total_amount: number;
  exit_commission: number;
  exit_stamp_duty: number;
  first_exit_at: number;
  last_exit_at: number;
  realized_pnl: number;
  net_pnl: number;
  max_unrealized_pnl: number;
  max_drawdown_pnl: number;
  hold_bars: number;
  hold_calendar_days: number;
  close_reason: string;
  entry_signal_ids: string[];
  exit_signal_ids: string[];
  entry_fill_ids: string[];
  exit_fill_ids: string[];
  entry_order_ids: string[];
  exit_order_ids: string[];
  linked_signal_ids: string[];
  linked_order_ids: string[];
  created_at: number;
  closed_at: number;
}

export interface TradingOrderPayload {
  order_id: number;
  code: string;
  order_type: string;
  volume: number;
  price: number;
  traded_volume: number;
  traded_price: number;   // 成交均价
  status: string;
  status_msg: string;     // 委托状态描述（废单原因等）
  order_time: string;
  remark: string;
  strategy_name: string;
}

export interface TradingExecutionOrderPayload {
  order_id: string;
  signal_id: string;
  strategy_id: string;
  account_id: string;
  code: string;
  direction: string;
  volume: number;
  price: number;
  submitted_at: number;
  status: string;
  is_active: boolean;
  is_terminal: boolean;
}

export interface TradingTradePayload {
  code: string;
  order_type: string;
  volume: number;
  price: number;
  amount: number;
  time: string;
  order_id: number;
  trade_id: string;
  strategy_name: string;
  remark: string;
}

export interface PlaceOrderResult {
  order_id: number | null;
  status: string;
  msg: string;
  attached_cond_ids?: string[];
}

export interface CancelOrderResult {
  order_id: number;
  account_id: string;
  cancelled: boolean;
}

export interface ReplaceTradingOrderResult {
  source_order_id: number;
  account_id: string;
  code: string;
  direction: string;
  requested_price: number;
  requested_volume: number;
  remaining_volume_before_replace: number;
  price_type: string;
  signal_id: string;
  cancelled: boolean;
  replacement_order_id: number | null;
  replaced: boolean;
  status: string;
  msg: string;
}

export async function fetchTradingAccountAsset(
  accountId: string,
  signal?: AbortSignal,
): Promise<TradingAccountAssetPayload> {
  return requestJson<TradingAccountAssetPayload>(
    `/api/v1/trading/accounts/${encodeURIComponent(accountId)}/asset`,
    { signal },
  );
}

export async function fetchTradingPositions(
  accountId: string,
  signal?: AbortSignal,
): Promise<TradingPositionPayload[]> {
  const q = new URLSearchParams({ account_id: accountId });
  return requestJson<TradingPositionPayload[]>(`/api/v1/trading/positions?${q}`, { signal });
}

export async function fetchTradingPositionLifecycle(
  accountId: string,
  status: "open" | "closed" | "all" = "open",
  signal?: AbortSignal,
  params: { code?: string; executionMode?: string; strategyId?: string; limit?: number } = {},
): Promise<{ items: TradingPositionLifecyclePayload[]; total: number }> {
  const q = new URLSearchParams({
    account_id: accountId,
    status,
    execution_mode: params.executionMode ?? "live",
    limit: String(params.limit ?? 200),
  });
  if (params.code) {
    q.set("code", params.code);
  }
  if (params.strategyId) {
    q.set("strategy_id", params.strategyId);
  }
  return requestJson<{ items: TradingPositionLifecyclePayload[]; total: number }>(
    `/api/v1/trading/position-lifecycle?${q}`,
    { signal },
  );
}

export async function fetchTradingOrders(
  accountId: string,
  cancelableOnly = false,
  signal?: AbortSignal,
): Promise<TradingOrderPayload[]> {
  const q = new URLSearchParams({ account_id: accountId });
  if (cancelableOnly) q.set("cancelable_only", "true");
  return requestJson<TradingOrderPayload[]>(`/api/v1/trading/orders?${q}`, { signal });
}

export async function fetchTradingExecutionOrders(
  accountId: string,
  status: "active" | "terminal" | "all" = "active",
  signal?: AbortSignal,
  params: { code?: string; limit?: number } = {},
): Promise<{ items: TradingExecutionOrderPayload[]; total: number }> {
  const q = new URLSearchParams({
    account_id: accountId,
    status,
    limit: String(params.limit ?? 200),
  });
  if (params.code) {
    q.set("code", params.code);
  }
  return requestJson<{ items: TradingExecutionOrderPayload[]; total: number }>(
    `/api/v1/trading/execution-orders?${q}`,
    { signal },
  );
}

export async function fetchTradingTrades(
  accountId: string,
  signal?: AbortSignal,
): Promise<TradingTradePayload[]> {
  const q = new URLSearchParams({ account_id: accountId });
  return requestJson<TradingTradePayload[]>(`/api/v1/trading/trades?${q}`, { signal });
}

export interface TradingLedgerEntry {
  traded_id: string;
  account_id: string;
  order_id: number;
  order_sysid: string;
  stock_code: string;
  order_type: number;
  direction_label: string;
  traded_time: number;
  traded_price: number;
  traded_volume: number;
  traded_amount: number;
  strategy_name: string;
  order_remark: string;
  session_date: string;
  archived_at: string;
  signal_id: string;
}

export interface TradingAuditSignalEntry {
  signal_id: string;
  strategy_id: string;
  code: string;
  direction: string;
  price_hint: number | null;
  volume_hint: number | null;
  created_at: number;
  account_id: string;
}

export interface TradingAuditOrderEntry {
  order_id: string;
  signal_id: string;
  code: string;
  direction: string;
  volume: number | null;
  price: number | null;
  submitted_at: number;
  status: string;
  account_id: string;
}

export interface TradingAuditFillEntry {
  fill_id: string;
  order_id: string;
  filled_at: number;
  filled_price: number | null;
  filled_volume: number | null;
  pnl_snapshot: number | null;
  account_id: string;
}

export interface TradingAuditChainPayload {
  signal_id: string;
  signal: TradingAuditSignalEntry | null;
  orders: TradingAuditOrderEntry[];
  fills: TradingAuditFillEntry[];
  server_time: number;
  build_version?: string;
  commit_sha?: string;
}

export async function fetchTradingLedger(
  accountId: string,
  startDate?: string,
  endDate?: string,
  code?: string,
  limit = 200,
  offset = 0,
  signal?: AbortSignal,
): Promise<{ records: TradingLedgerEntry[]; total: number }> {
  const q = new URLSearchParams({ account_id: accountId });
  if (startDate) q.set("start_date", startDate);
  if (endDate) q.set("end_date", endDate);
  if (code) q.set("code", code);
  q.set("limit", String(limit));
  q.set("offset", String(offset));
  return requestJson<{ records: TradingLedgerEntry[]; total: number }>(
    `/api/v1/trading/ledger?${q}`,
    { signal },
  );
}

export async function fetchTradingAuditChain(
  signalId: string,
  accountId?: string,
  signal?: AbortSignal,
): Promise<TradingAuditChainPayload> {
  const q = new URLSearchParams();
  if (accountId) q.set("account_id", accountId);
  const suffix = q.size > 0 ? `?${q.toString()}` : "";
  return requestJson<TradingAuditChainPayload>(
    `/api/v1/trading/audit-chain/${encodeURIComponent(signalId)}${suffix}`,
    { signal },
  );
}

export async function placeOrder(
  accountId: string,
  code: string,
  direction: "buy" | "sell",
  volume: number,
  price = 0,
  priceType = "market",
  signalId = "",
  tpPrice?: number,
  slPrice?: number,
): Promise<PlaceOrderResult> {
  return requestJson<PlaceOrderResult>("/api/v1/trading/orders", {
    method: "POST",
    body: JSON.stringify({
      account_id: accountId,
      code,
      direction,
      volume,
      price,
      price_type: priceType,
      signal_id: signalId,
      ...(tpPrice != null ? { tp_price: tpPrice } : {}),
      ...(slPrice != null ? { sl_price: slPrice } : {}),
    }),
  });
}

export async function cancelOrder(
  accountId: string,
  orderId: number,
): Promise<CancelOrderResult> {
  const q = new URLSearchParams({ account_id: accountId });
  return requestJson<CancelOrderResult>(`/api/v1/trading/orders/${orderId}?${q}`, {
    method: "DELETE",
  });
}

export async function replaceTradingOrder(
  accountId: string,
  orderId: string | number,
  price: number,
  params: { priceType?: string; volume?: number; signalId?: string } = {},
): Promise<ReplaceTradingOrderResult> {
  return requestJson<ReplaceTradingOrderResult>(`/api/v1/trading/orders/${encodeURIComponent(String(orderId))}/replace`, {
    method: "POST",
    body: JSON.stringify({
      account_id: accountId,
      price,
      price_type: params.priceType ?? "limit",
      volume: params.volume,
      signal_id: params.signalId ?? "",
    }),
  });
}

export interface CancelAllOrdersResult {
  cancelled: number;
  failed: number;
  order_ids: number[];
}

export async function cancelAllOrders(accountId: string): Promise<CancelAllOrdersResult> {
  const q = new URLSearchParams({ account_id: accountId });
  return requestJson<CancelAllOrdersResult>(`/api/v1/trading/orders?${q}`, { method: "DELETE" });
}

// ── 条件单（止损 / 止盈） ────────────────────────────────────────────

export interface CondOrderPayload {
  id: string;
  account_id: string;
  symbol: string;
  direction: "buy" | "sell";
  volume: number;
  order_price: number;
  price_type: string;
  trigger_price: number;
  trigger_type: "gte" | "lte";  // gte=价格>=触发价, lte=价格<=触发价
  note: string;
  status: "pending" | "triggered" | "cancelled" | "error";
  triggered_price: number | null;
  result_order_id: number | null;
  error_msg: string | null;
  created_at: string;
  triggered_at: string | null;
}

export interface PlaceCondOrderBody {
  account_id: string;
  symbol: string;
  direction: "buy" | "sell";
  volume: number;
  order_price?: number;
  price_type?: string;
  trigger_price: number;
  trigger_type: "gte" | "lte";
  trigger_price_source?: "last" | "mark" | "index";
  tp_price?: number;
  sl_price?: number;
  note?: string;
}

export async function fetchCondOrders(
  accountId?: string,
  signal?: AbortSignal,
): Promise<CondOrderPayload[]> {
  const q = new URLSearchParams();
  if (accountId) q.set("account_id", accountId);
  return requestJson<CondOrderPayload[]>(`/api/v1/trading/cond-orders?${q}`, { signal });
}

export async function placeCondOrder(body: PlaceCondOrderBody): Promise<CondOrderPayload> {
  return requestJson<CondOrderPayload>("/api/v1/trading/cond-orders", {
    method: "POST",
    body: JSON.stringify(body),
  });
}

export async function cancelCondOrder(condId: string): Promise<{ id: string; cancelled: boolean }> {
  return requestJson<{ id: string; cancelled: boolean }>(
    `/api/v1/trading/cond-orders/${encodeURIComponent(condId)}`,
    { method: "DELETE" },
  );
}

// ── 交易状态 / 账户发现 ─────────────────────────────────────────────

export interface TradingAccountStatusItem {
  id: string;
  label: string;
  trade_account: string;
  status: "connected" | "disconnected" | "disabled";
  hint?: string;
}

export interface TradingStatusPayload {
  accounts: TradingAccountStatusItem[];
}

export async function fetchTradingStatus(signal?: AbortSignal): Promise<TradingStatusPayload> {
  return requestJson<TradingStatusPayload>("/api/v1/trading/status", { signal });
}

export interface TradingAccountConnectPayload {
  account_id: string;
  status: "connected";
  userdata_path: string;
  message: string;
}

export async function connectTradingAccount(
  accountId: string,
  signal?: AbortSignal,
): Promise<TradingAccountConnectPayload> {
  return requestJson<TradingAccountConnectPayload>(
    `/api/v1/trading/accounts/${encodeURIComponent(accountId)}/connect`,
    { method: "POST", signal },
  );
}

export interface DiscoveredAccount {
  userdata_path: string;
  account_id: string;
  account_type: string;
  account_type_int: number;
  broker_id: string;
  login_status: number;
}

export interface DiscoverAccountsResult {
  discovered: DiscoveredAccount[];
  errors: string[];
}

export async function discoverTradingAccounts(
  userdataPath?: string,
): Promise<DiscoverAccountsResult> {
  return requestJson<DiscoverAccountsResult>("/api/v1/trading/discover-accounts", {
    method: "POST",
    body: JSON.stringify(userdataPath ? { userdata_path: userdataPath } : {}),
  });
}

// ---------------------------------------------------------------------------
// 资产域 — 财务数据
// ---------------------------------------------------------------------------

export interface FinancialDataPayload {
  stock_code: string;
  start_date: string;
  end_date: string;
  server_time: number;
  income?: Record<string, unknown>[];
  balance?: Record<string, unknown>[];
  cashflow?: Record<string, unknown>[];
}

export interface FinancialRefreshPayload {
  success: boolean;
  stock_code?: string;
  source?: string;
  message?: string;
  [key: string]: unknown;
}

export async function fetchFinancialData(
  stockCode: string,
  params: { startDate?: string; endDate?: string; table?: "income" | "balance" | "cashflow" } = {},
  signal?: AbortSignal,
): Promise<FinancialDataPayload> {
  const query = new URLSearchParams();
  if (params.startDate) query.set("start_date", params.startDate);
  if (params.endDate) query.set("end_date", params.endDate);
  if (params.table) query.set("table", params.table);
  const qs = query.toString();
  return requestJson<FinancialDataPayload>(
    `/api/v1/data/financial/${encodeURIComponent(stockCode)}${qs ? `?${qs}` : ""}`,
    { method: "GET", signal },
  );
}

export async function triggerFinancialRefresh(
  stockCode: string,
  params: { startDate?: string; endDate?: string } = {},
  signal?: AbortSignal,
): Promise<FinancialRefreshPayload> {
  const query = new URLSearchParams();
  if (params.startDate) query.set("start_date", params.startDate);
  if (params.endDate) query.set("end_date", params.endDate);
  const qs = query.toString();
  return requestJson<FinancialRefreshPayload>(
    `/api/v1/data/financial/${encodeURIComponent(stockCode)}/refresh${qs ? `?${qs}` : ""}`,
    { method: "POST", signal },
  );
}

// ---------------------------------------------------------------------------
// 基础历史数据弹药库 — 完整性验收摘要
// ---------------------------------------------------------------------------

export interface BasicArsenalPeriodStatus {
  period: string;
  table: string;
  covered_symbols: number;
  expected_symbols: number;
  coverage_ratio: number | null;
  fresh_symbols_count: number;
  fresh_ratio: number | null;
  total_bars: number;
  earliest_bar: string | null;
  latest_bar: string | null;
  latest_trade_day: string | null;
  latest_trade_day_lag: number | null;
  missing_symbols_count: number;
  missing_symbols_sample: string[];
  stale_symbols_count: number;
  stale_symbols_sample: string[];
  gate_pass_symbols: number;
  gate_reject_symbols: number;
  structurally_ready: boolean;
  ready: boolean;
  error: string | null;
}

export interface BasicArsenalStatusPayload {
  periods: BasicArsenalPeriodStatus[];
  ready_count: number;
  structurally_ready_count: number;
  total_periods: number;
  structurally_ready: boolean;
  accepted: boolean;
  duckdb_error: string | null;
  structural_coverage_threshold: number;
  structural_max_lag_days: number;
  target_universe_source: string;
  target_universe_size: number;
  target_universe_sample: string[];
  target_universe_note: string | null;
  as_of_trade_day: string | null;
  acceptance_reasons: string[];
  blocking_issues: string[];
  cache_status?: "fresh" | "miss" | "stale";
  cache_age_ms?: number;
  server_time: number;
}

export async function fetchBasicArsenalStatus(
  signal?: AbortSignal,
  options: { force?: boolean } = {},
): Promise<BasicArsenalStatusPayload> {
  const query = new URLSearchParams();
  if (options.force) {
    query.set("force", "true");
  }
  const suffix = query.size > 0 ? `?${query.toString()}` : "";
  return requestJson<BasicArsenalStatusPayload>(`/api/v1/data-quality/basic-arsenal-status${suffix}`, { signal });
}
