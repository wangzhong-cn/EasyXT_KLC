class Events:
    CONNECTION_STATUS_CHANGED = "connection_status_changed"
    BACKTEST_ENGINE_STATUS_UPDATED = "backtest_engine_status_updated"
    REALTIME_PIPELINE_STATUS_UPDATED = "realtime_pipeline_status_updated"
    SYMBOL_SELECTED = "symbol_selected"
    CHART_DATA_LOADED = "chart_data_loaded"
    PERIOD_CHANGED = "period_changed"
    ORDER_SUBMITTED = "order_submitted"
    ORDER_REQUESTED = "order_requested"
    ORDER_BATCH_REQUESTED = "order_batch_requested"
    ORDER_FILLED = "order_filled"
    POSITION_UPDATED = "position_updated"
    CHART_PRICE_CLICKED = "chart_price_clicked"
    ACCOUNT_UPDATED = "account_updated"
    STRATEGY_STARTED = "strategy_started"
    STRATEGY_STOPPED = "strategy_stopped"
    SIGNAL_GENERATED = "signal_generated"
    DATA_DOWNLOADED = "data_downloaded"
    DATA_UPDATED = "data_updated"
    BACKFILL_TASK_UPDATED = "backfill_task_updated"
    DATA_QUALITY_ALERT = "data_quality_alert"
    TRADING_INTERFACE_READY = "trading_interface_ready"
    # 图表子系统事件（Stage 2/3 chart migration）
    CHART_SLO_ALERT = "chart_slo_alert"
    CHART_BACKEND_SWITCHED = "chart_backend_switched"
    # 风控引擎事件（Phase 1）
    RISK_ALERT = "risk_alert"
    RISK_LIMIT_BREACHED = "risk_limit_breached"
    STRATEGY_RISK_TRIGGERED = "strategy_risk_triggered"
    # 数据治理面板事件
    DATA_REPAIRED = "data_repaired"
    ENV_CONFIG_SAVED = "env_config_saved"
    # 审计链路事件（Phase 1）
    AUDIT_ENTRY_CREATED = "audit_entry_created"
    # 策略生命周期事件（Phase 1）
    STRATEGY_LIFECYCLE = "strategy_lifecycle"
    # 线程退出安全事件：线程被强制终止时上报，便于统计"强杀频率"
    THREAD_FORCED_TERMINATE = "thread_forced_terminate"
    # 全周期数据入库完成事件（payload: stock_codes, periods, success_count, failed_count）
    DATA_INGESTION_COMPLETE = "data_ingestion_complete"
    # 数据覆盖率矩阵更新（payload: coverage_df_json）
    DATA_COVERAGE_UPDATED = "data_coverage_updated"
    # 批量下载进度（payload: current, total, stock_code, period, status）
    BULK_DOWNLOAD_PROGRESS = "bulk_download_progress"
