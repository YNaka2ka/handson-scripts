/*
Taskセクションのポイント:

Task DAG: 依存関係を持つTaskチェーンの構築
CRON スケジュール: 平日18時に自動実行
エラーハンドリング: 各Taskで処理結果をログテーブルに記録
段階的処理: データロード→分析→アラート生成の順次実行
監視機能: Task実行履歴とステータスの可視化
営業支援: 担当者別のアラートメール準備
手動実行: テスト用の即座実行機能
*/
-- ============================================
-- Task自動化セクション: 日次処理の自動化
-- ============================================

USE SCHEMA SECURITIES_WORKSHOP.STAGING;

-- ============================================
-- 1. 日次で市場データをロードするTask
-- ============================================
-- Task 1: 市場データの日次ロード
CREATE OR REPLACE TASK TASK_LOAD_MARKET_DATA
  WAREHOUSE = COMPUTE_WH
  SCHEDULE = 'USING CRON 0 18 * * MON-FRI Asia/Tokyo'  -- 平日18時に実行
  COMMENT = '市場データを日次でロードするTask'
AS
BEGIN
  -- 最新の市場データをロード
  COPY INTO SECURITIES_WORKSHOP.RAW_DATA.MARKET_PRICES (
    stock_code,
    stock_name,
    sector,
    price_date,
    closing_price,
    volume,
    week52_high,
    week52_low
  )
  FROM (
    SELECT 
      $1,
      $2,
      $3,
      TO_DATE($4, 'YYYY-MM-DD'),
      TO_NUMBER($5, 12, 2),
      TO_NUMBER($6),
      TO_NUMBER($7, 12, 2),
      TO_NUMBER($8, 12, 2)
    FROM @SECURITIES_WORKSHOP.RAW_DATA.WORKSHOP_STAGE/market_prices.csv
  )
  FILE_FORMAT = SECURITIES_WORKSHOP.RAW_DATA.CSV_FORMAT
  ON_ERROR = 'CONTINUE';
  
  -- ロード履歴をログテーブルに記録
  INSERT INTO SECURITIES_WORKSHOP.STAGING.DATA_QUALITY_LOG (
    table_name,
    check_type,
    check_result,
    affected_rows,
    details
  )
  SELECT 
    'MARKET_PRICES' AS table_name,
    'DAILY_LOAD' AS check_type,
    'SUCCESS' AS check_result,
    row_count AS affected_rows,
    CONCAT('Loaded from file: ', file_name) AS details
  FROM TABLE(INFORMATION_SCHEMA.COPY_HISTORY(
    TABLE_NAME => 'SECURITIES_WORKSHOP.RAW_DATA.MARKET_PRICES',
    START_TIME => DATEADD(MINUTE, -5, CURRENT_TIMESTAMP())
  ))
  WHERE status = 'LOADED';
END;

-- Task確認
SHOW TASKS LIKE 'TASK_LOAD_MARKET_DATA';

-- ============================================
-- 2. ポートフォリオ評価を更新するTask
-- ============================================

-- Task 2: 日次ポートフォリオスナップショット更新
CREATE OR REPLACE TASK TASK_UPDATE_PORTFOLIO_SNAPSHOT
  WAREHOUSE = COMPUTE_WH
  COMMENT = 'ポートフォリオスナップショットを日次で更新するTask'
  AFTER TASK_LOAD_MARKET_DATA
AS
BEGIN
  -- ポートフォリオスナップショットをMERGE
  MERGE INTO SECURITIES_WORKSHOP.ANALYTICS.PORTFOLIO_SNAPSHOT ps
  USING (
    SELECT 
      CURRENT_DATE() AS snapshot_date,
      h.customer_id,
      h.stock_code,
      mp.stock_name,
      mp.sector,
      h.quantity,
      h.avg_purchase_price,
      mp.closing_price AS current_price,
      h.quantity * mp.closing_price AS market_value,
      (h.quantity * mp.closing_price) - (h.quantity * h.avg_purchase_price) AS unrealized_pl,
      ROUND(
        ((h.quantity * mp.closing_price) - (h.quantity * h.avg_purchase_price)) 
        / NULLIF(h.quantity * h.avg_purchase_price, 0) * 100, 
        4
      ) AS unrealized_pl_pct
    FROM SECURITIES_WORKSHOP.RAW_DATA.HOLDINGS h
    JOIN SECURITIES_WORKSHOP.ANALYTICS.V_LATEST_PRICES mp 
      ON h.stock_code = mp.stock_code
  ) src
  ON ps.snapshot_date = src.snapshot_date 
     AND ps.customer_id = src.customer_id 
     AND ps.stock_code = src.stock_code
  WHEN MATCHED THEN 
    UPDATE SET
      ps.stock_name = src.stock_name,
      ps.sector = src.sector,
      ps.quantity = src.quantity,
      ps.avg_purchase_price = src.avg_purchase_price,
      ps.current_price = src.current_price,
      ps.market_value = src.market_value,
      ps.unrealized_pl = src.unrealized_pl,
      ps.unrealized_pl_pct = src.unrealized_pl_pct,
      ps.load_timestamp = CURRENT_TIMESTAMP()
  WHEN NOT MATCHED THEN
    INSERT (
      snapshot_date,
      customer_id,
      stock_code,
      stock_name,
      sector,
      quantity,
      avg_purchase_price,
      current_price,
      market_value,
      unrealized_pl,
      unrealized_pl_pct
    )
    VALUES (
      src.snapshot_date,
      src.customer_id,
      src.stock_code,
      src.stock_name,
      src.sector,
      src.quantity,
      src.avg_purchase_price,
      src.current_price,
      src.market_value,
      src.unrealized_pl,
      src.unrealized_pl_pct
    );
  
  -- 処理結果をログに記録
  INSERT INTO SECURITIES_WORKSHOP.STAGING.DATA_QUALITY_LOG (
    table_name,
    check_type,
    check_result,
    affected_rows,
    details
  )
  SELECT 
    'PORTFOLIO_SNAPSHOT',
    'DAILY_UPDATE',
    'SUCCESS',
    COUNT(*),
    CONCAT('Updated snapshot for date: ', CURRENT_DATE())
  FROM SECURITIES_WORKSHOP.ANALYTICS.PORTFOLIO_SNAPSHOT
  WHERE snapshot_date = CURRENT_DATE();
END;

-- Task確認
SHOW TASKS LIKE 'TASK_UPDATE_PORTFOLIO_SNAPSHOT';

-- ============================================
-- 3. 顧客別アセットアロケーション更新Task
-- ============================================

-- Task 3: 顧客別アセットアロケーション更新
CREATE OR REPLACE TASK TASK_UPDATE_CUSTOMER_ALLOCATION
  WAREHOUSE = COMPUTE_WH
  COMMENT = '顧客別アセットアロケーションを更新し、乖離を計算するTask'
  AFTER TASK_UPDATE_PORTFOLIO_SNAPSHOT  -- ポートフォリオスナップショット更新後に実行
AS
BEGIN
  -- アセットアロケーションをMERGE
  MERGE INTO SECURITIES_WORKSHOP.ANALYTICS.CUSTOMER_ALLOCATION ca
  USING (
    WITH sector_allocation AS (
      SELECT 
        ps.snapshot_date,
        ps.customer_id,
        c.risk_tolerance,
        ps.sector,
        SUM(ps.market_value) AS sector_market_value
      FROM SECURITIES_WORKSHOP.ANALYTICS.PORTFOLIO_SNAPSHOT ps
      JOIN SECURITIES_WORKSHOP.RAW_DATA.CUSTOMERS c 
        ON ps.customer_id = c.customer_id
      WHERE ps.snapshot_date = CURRENT_DATE()
      GROUP BY ps.snapshot_date, ps.customer_id, c.risk_tolerance, ps.sector
    ),
    customer_total AS (
      SELECT 
        snapshot_date,
        customer_id,
        SUM(sector_market_value) AS total_market_value
      FROM sector_allocation
      GROUP BY snapshot_date, customer_id
    )
    SELECT 
      sa.snapshot_date,
      sa.customer_id,
      sa.risk_tolerance,
      sa.sector,
      sa.sector_market_value AS market_value,
      ROUND(sa.sector_market_value / ct.total_market_value * 100, 2) AS allocation_ratio,
      COALESCE(ta.target_ratio, 0) AS target_ratio,
      ROUND(
        (sa.sector_market_value / ct.total_market_value * 100) - COALESCE(ta.target_ratio, 0), 
        2
      ) AS deviation,
      CASE 
        WHEN ABS((sa.sector_market_value / ct.total_market_value * 100) - COALESCE(ta.target_ratio, 0)) > 5 
        THEN TRUE 
        ELSE FALSE 
      END AS rebalance_flag
    FROM sector_allocation sa
    JOIN customer_total ct 
      ON sa.snapshot_date = ct.snapshot_date 
      AND sa.customer_id = ct.customer_id
    LEFT JOIN SECURITIES_WORKSHOP.RAW_DATA.TARGET_ALLOCATION ta 
      ON sa.risk_tolerance = ta.risk_tolerance 
      AND sa.sector = ta.sector
  ) src
  ON ca.snapshot_date = src.snapshot_date 
     AND ca.customer_id = src.customer_id 
     AND ca.sector = src.sector
  WHEN MATCHED THEN 
    UPDATE SET
      ca.risk_tolerance = src.risk_tolerance,
      ca.market_value = src.market_value,
      ca.allocation_ratio = src.allocation_ratio,
      ca.target_ratio = src.target_ratio,
      ca.deviation = src.deviation,
      ca.rebalance_flag = src.rebalance_flag,
      ca.load_timestamp = CURRENT_TIMESTAMP()
  WHEN NOT MATCHED THEN
    INSERT (
      snapshot_date,
      customer_id,
      risk_tolerance,
      sector,
      market_value,
      allocation_ratio,
      target_ratio,
      deviation,
      rebalance_flag
    )
    VALUES (
      src.snapshot_date,
      src.customer_id,
      src.risk_tolerance,
      src.sector,
      src.market_value,
      src.allocation_ratio,
      src.target_ratio,
      src.deviation,
      src.rebalance_flag
    );
  
  -- 処理結果をログに記録
  INSERT INTO SECURITIES_WORKSHOP.STAGING.DATA_QUALITY_LOG (
    table_name,
    check_type,
    check_result,
    affected_rows,
    details
  )
  SELECT 
    'CUSTOMER_ALLOCATION',
    'DAILY_UPDATE',
    'SUCCESS',
    COUNT(*),
    CONCAT('Updated allocation for date: ', CURRENT_DATE())
  FROM SECURITIES_WORKSHOP.ANALYTICS.CUSTOMER_ALLOCATION
  WHERE snapshot_date = CURRENT_DATE();
END;

-- Task確認
SHOW TASKS LIKE 'TASK_UPDATE_CUSTOMER_ALLOCATION';

-- ============================================
-- 4. リバランスアラート生成Task
-- ============================================

-- Task 4: リバランスアラート生成
CREATE OR REPLACE TASK TASK_GENERATE_REBALANCE_ALERTS
  WAREHOUSE = COMPUTE_WH
  COMMENT = 'リバランスが必要な顧客のアラートを生成するTask'
  AFTER TASK_UPDATE_CUSTOMER_ALLOCATION  -- アロケーション更新後に実行
AS
BEGIN
  -- 既存の本日のアラートを一旦CLOSEDに更新
  UPDATE SECURITIES_WORKSHOP.ANALYTICS.REBALANCE_ALERTS
  SET alert_status = 'CLOSED'
  WHERE alert_date = CURRENT_DATE()
    AND alert_status = 'OPEN';
  
  -- 新規アラート生成
  INSERT INTO SECURITIES_WORKSHOP.ANALYTICS.REBALANCE_ALERTS (
    alert_date,
    customer_id,
    customer_name,
    risk_tolerance,
    sales_rep,
    sector,
    current_ratio,
    target_ratio,
    deviation,
    recommended_action
  )
  SELECT 
    CURRENT_DATE() AS alert_date,
    ca.customer_id,
    c.customer_name,
    ca.risk_tolerance,
    c.sales_rep,
    ca.sector,
    ca.allocation_ratio AS current_ratio,
    ca.target_ratio,
    ca.deviation,
    CASE 
      WHEN ca.deviation > 5 THEN 
        CONCAT('【売却推奨】', ca.sector, 'が目標比率より', ROUND(ca.deviation, 1), '%過大です。一部売却をご検討ください。')
      WHEN ca.deviation < -5 THEN 
        CONCAT('【買付推奨】', ca.sector, 'が目標比率より', ROUND(ABS(ca.deviation), 1), '%不足です。追加購入をご検討ください。')
    END AS recommended_action
  FROM SECURITIES_WORKSHOP.ANALYTICS.CUSTOMER_ALLOCATION ca
  JOIN SECURITIES_WORKSHOP.RAW_DATA.CUSTOMERS c 
    ON ca.customer_id = c.customer_id
  WHERE ca.snapshot_date = CURRENT_DATE()
    AND ca.rebalance_flag = TRUE;
  
  -- 処理結果をログに記録
  INSERT INTO SECURITIES_WORKSHOP.STAGING.DATA_QUALITY_LOG (
    table_name,
    check_type,
    check_result,
    affected_rows,
    details
  )
  SELECT 
    'REBALANCE_ALERTS',
    'ALERT_GENERATION',
    'SUCCESS',
    COUNT(*),
    CONCAT('Generated ', COUNT(*), ' alerts for date: ', CURRENT_DATE())
  FROM SECURITIES_WORKSHOP.ANALYTICS.REBALANCE_ALERTS
  WHERE alert_date = CURRENT_DATE()
    AND alert_status = 'OPEN';
END;

-- Task確認
SHOW TASKS LIKE 'TASK_GENERATE_REBALANCE_ALERTS';

-- ============================================
-- 5. リバランスアラートメール送信Task（営業担当者向け）
-- ============================================

-- 営業担当者別アラートサマリービュー作成
CREATE OR REPLACE VIEW SECURITIES_WORKSHOP.ANALYTICS.V_SALES_ALERT_SUMMARY AS
SELECT 
  ra.sales_rep,
  ra.alert_date,
  COUNT(DISTINCT ra.customer_id) AS customers_need_action,
  COUNT(*) AS total_alerts,
  LISTAGG(DISTINCT ra.customer_name, ', ') WITHIN GROUP (ORDER BY ra.customer_name) AS customer_list,
  ROUND(AVG(ABS(ra.deviation)), 2) AS avg_deviation,
  MAX(ABS(ra.deviation)) AS max_deviation
FROM SECURITIES_WORKSHOP.ANALYTICS.REBALANCE_ALERTS ra
WHERE ra.alert_status = 'OPEN'
GROUP BY ra.sales_rep, ra.alert_date;

-- Task 5: メール送信準備（アラートサマリーテーブル作成）
CREATE OR REPLACE TASK TASK_PREPARE_ALERT_EMAIL
  WAREHOUSE = COMPUTE_WH
  COMMENT = '営業担当者向けアラートメールデータを準備するTask'
  AFTER TASK_GENERATE_REBALANCE_ALERTS  -- アラート生成後に実行
AS
BEGIN
  -- 営業担当者別の詳細アラートレポート作成
  CREATE OR REPLACE TEMPORARY TABLE TEMP_EMAIL_REPORT AS
  SELECT 
    sales_rep,
    customer_id,
    customer_name,
    risk_tolerance,
    sector,
    current_ratio,
    target_ratio,
    deviation,
    recommended_action
  FROM SECURITIES_WORKSHOP.ANALYTICS.REBALANCE_ALERTS
  WHERE alert_date = CURRENT_DATE()
    AND alert_status = 'OPEN'
  ORDER BY sales_rep, ABS(deviation) DESC;
  
  -- メール送信ログ（実際のメール送信機能は別途実装）
  INSERT INTO SECURITIES_WORKSHOP.STAGING.DATA_QUALITY_LOG (
    table_name,
    check_type,
    check_result,
    affected_rows,
    details
  )
  SELECT 
    'EMAIL_ALERTS',
    'EMAIL_PREPARATION',
    'SUCCESS',
    COUNT(DISTINCT sales_rep),
    CONCAT('Prepared email alerts for ', COUNT(DISTINCT sales_rep), ' sales representatives')
  FROM TEMP_EMAIL_REPORT;
END;

-- Task確認
SHOW TASKS LIKE 'TASK_PREPARE_ALERT_EMAIL';

-- ============================================
-- 6. メール本文生成用ストアドプロシージャ
-- ============================================

CREATE OR REPLACE PROCEDURE SP_GENERATE_ALERT_EMAIL_BODY(sales_rep_name VARCHAR)
RETURNS VARCHAR
LANGUAGE SQL
AS
$$
DECLARE
  email_body VARCHAR;
  alert_count NUMBER;
  customer_count NUMBER;
  rep_name VARCHAR DEFAULT sales_rep_name;
BEGIN
  SELECT 
    COUNT(*),
    COUNT(DISTINCT customer_id)
  INTO 
    alert_count,
    customer_count
  FROM SECURITIES_WORKSHOP.ANALYTICS.REBALANCE_ALERTS
  WHERE sales_rep = :rep_name
    AND alert_date = CURRENT_DATE()
    AND alert_status = 'OPEN';
  
  email_body := '
【ポートフォリオリバランスアラート】' || CHAR(10) || CHAR(10) ||
:rep_name || ' 様' || CHAR(10) || CHAR(10) ||
'本日のリバランスアラートをお知らせします。' || CHAR(10) ||
'対応が必要な顧客: ' || customer_count || '名' || CHAR(10) ||
'アラート件数: ' || alert_count || '件' || CHAR(10) || CHAR(10) ||
'【詳細】' || CHAR(10) ||
'以下の顧客に対してリバランス提案が必要です。' || CHAR(10) || CHAR(10);
  
  LET detail_cursor CURSOR FOR
    SELECT 
      customer_name,
      sector,
      deviation,
      recommended_action
    FROM SECURITIES_WORKSHOP.ANALYTICS.REBALANCE_ALERTS
    WHERE sales_rep = ?
      AND alert_date = CURRENT_DATE()
      AND alert_status = 'OPEN'
    ORDER BY ABS(deviation) DESC
    LIMIT 10;
  
  OPEN detail_cursor USING (:rep_name);
  FOR record IN detail_cursor DO
    email_body := email_body || 
      '・' || record.customer_name || ' / ' || record.sector || 
      ' (乖離: ' || ROUND(record.deviation, 1) || '%)' || CHAR(10) ||
      '  ' || record.recommended_action || CHAR(10) || CHAR(10);
  END FOR;
  
  email_body := email_body || 
'詳細はポートフォリオ管理システムをご確認ください。' || CHAR(10) || CHAR(10) ||
'※このメールは自動送信されています。';
  
  RETURN email_body;
END;
$$;

-- ストアドプロシージャのテスト実行
CALL SP_GENERATE_ALERT_EMAIL_BODY('田中一郎');

-- ============================================
-- 7. Task依存関係の確認と実行順序の可視化
-- ============================================

-- Task依存関係の確認
SELECT 
  name,
  database_name,
  schema_name,
  state,
  schedule,
  predecessors,
  CASE 
    WHEN state = 'suspended' THEN '⏸️ 一時停止中'
    WHEN state = 'started' THEN '▶️ 実行中'
    ELSE state
  END AS status_icon,
  created_on,
  comment
FROM TABLE(INFORMATION_SCHEMA.TASK_DEPENDENTS(
  TASK_NAME => 'SECURITIES_WORKSHOP.STAGING.TASK_LOAD_MARKET_DATA',
  RECURSIVE => TRUE
))
ORDER BY created_on;

-- Task実行履歴の確認用ビュー
CREATE OR REPLACE VIEW SECURITIES_WORKSHOP.ANALYTICS.V_TASK_EXECUTION_HISTORY AS
SELECT 
  name AS task_name,
  state,
  scheduled_time,
  completed_time,
  DATEDIFF('second', scheduled_time, completed_time) AS execution_seconds,
  CASE 
    WHEN state = 'SUCCEEDED' THEN '✅ 成功'
    WHEN state = 'FAILED' THEN '❌ 失敗'
    WHEN state = 'SKIPPED' THEN '⏭️ スキップ'
    ELSE state
  END AS status_display,
  error_code,
  error_message
FROM TABLE(INFORMATION_SCHEMA.TASK_HISTORY(
  SCHEDULED_TIME_RANGE_START => DATEADD('day', -7, CURRENT_TIMESTAMP()),
  RESULT_LIMIT => 100
))
WHERE database_name = 'SECURITIES_WORKSHOP'
ORDER BY scheduled_time DESC;

-- ============================================
-- 8. Taskの有効化（実際に実行開始）
-- ============================================

-- ⚠️ 注意: Taskを有効化すると自動実行が開始されます

-- 末端のTaskから順に有効化（逆順）
ALTER TASK TASK_PREPARE_ALERT_EMAIL RESUME;
ALTER TASK TASK_GENERATE_REBALANCE_ALERTS RESUME;
ALTER TASK TASK_UPDATE_CUSTOMER_ALLOCATION RESUME;
ALTER TASK TASK_UPDATE_PORTFOLIO_SNAPSHOT RESUME;
ALTER TASK TASK_LOAD_MARKET_DATA RESUME;  -- 最後にルートTaskを有効化

-- Task状態確認
SHOW TASKS IN SCHEMA SECURITIES_WORKSHOP.STAGING;

-- ============================================
-- 9. 手動でTaskを即座に実行（テスト用）
-- ============================================

-- 個別Taskの手動実行
EXECUTE TASK TASK_LOAD_MARKET_DATA;

-- 実行結果確認（数秒待ってから実行）
SELECT * FROM SECURITIES_WORKSHOP.ANALYTICS.V_TASK_EXECUTION_HISTORY
WHERE task_name = 'TASK_LOAD_MARKET_DATA'
ORDER BY scheduled_time DESC
LIMIT 5;

-- 全Taskチェーンの実行状況確認
SELECT 
  task_name,
  status_display,
  scheduled_time,
  completed_time,
  execution_seconds,
  error_message
FROM SECURITIES_WORKSHOP.ANALYTICS.V_TASK_EXECUTION_HISTORY
WHERE DATE(scheduled_time) = CURRENT_DATE()
ORDER BY scheduled_time DESC;

-- ============================================
-- 10. Task監視とアラート確認
-- ============================================

-- 本日のタスク実行サマリー
SELECT 
  DATE(scheduled_time) AS execution_date,
  COUNT(*) AS total_executions,
  SUM(CASE WHEN state = 'SUCCEEDED' THEN 1 ELSE 0 END) AS successful,
  SUM(CASE WHEN state = 'FAILED' THEN 1 ELSE 0 END) AS failed,
  SUM(CASE WHEN state = 'SKIPPED' THEN 1 ELSE 0 END) AS skipped,
  ROUND(AVG(execution_seconds), 2) AS avg_execution_seconds
FROM SECURITIES_WORKSHOP.ANALYTICS.V_TASK_EXECUTION_HISTORY
WHERE DATE(scheduled_time) = CURRENT_DATE()
GROUP BY DATE(scheduled_time);

-- 生成されたアラート確認
SELECT 
  sales_rep,
  customers_need_action,
  total_alerts,
  avg_deviation,
  customer_list
FROM SECURITIES_WORKSHOP.ANALYTICS.V_SALES_ALERT_SUMMARY
WHERE alert_date = CURRENT_DATE()
ORDER BY customers_need_action DESC;

-- 営業担当者別の詳細アラート
SELECT 
  sales_rep,
  customer_name,
  sector,
  current_ratio,
  target_ratio,
  deviation,
  recommended_action
FROM SECURITIES_WORKSHOP.ANALYTICS.REBALANCE_ALERTS
WHERE alert_date = CURRENT_DATE()
  AND alert_status = 'OPEN'
ORDER BY sales_rep, ABS(deviation) DESC;

-- ============================================
-- 11. Taskの一時停止と削除（必要に応じて）
-- ============================================

-- 全Taskを一時停止
-- ALTER TASK TASK_LOAD_MARKET_DATA SUSPEND;
-- ALTER TASK TASK_UPDATE_PORTFOLIO_SNAPSHOT SUSPEND;
-- ALTER TASK TASK_UPDATE_CUSTOMER_ALLOCATION SUSPEND;
-- ALTER TASK TASK_GENERATE_REBALANCE_ALERTS SUSPEND;
-- ALTER TASK TASK_PREPARE_ALERT_EMAIL SUSPEND;

-- Task削除（必要に応じて）
-- DROP TASK IF EXISTS TASK_PREPARE_ALERT_EMAIL;
-- DROP TASK IF EXISTS TASK_GENERATE_REBALANCE_ALERTS;
-- DROP TASK IF EXISTS TASK_UPDATE_CUSTOMER_ALLOCATION;
-- DROP TASK IF EXISTS TASK_UPDATE_PORTFOLIO_SNAPSHOT;
-- DROP TASK IF EXISTS TASK_LOAD_MARKET_DATA;

-- ============================================
-- 12. まとめ: Taskチェーン全体図
-- ============================================

/*
Taskの実行順序:

1. TASK_LOAD_MARKET_DATA (ルートTask)
   ↓ 
   スケジュール: 平日18時 (CRON)
   処理: 市場データのロード
   
2. TASK_UPDATE_PORTFOLIO_SNAPSHOT
   ↓
   依存: TASK_LOAD_MARKET_DATA完了後
   処理: ポートフォリオスナップショット更新
   
3. TASK_UPDATE_CUSTOMER_ALLOCATION
   ↓
   依存: TASK_UPDATE_PORTFOLIO_SNAPSHOT完了後
   処理: アセットアロケーション計算と乖離分析
   
4. TASK_GENERATE_REBALANCE_ALERTS
   ↓
   依存: TASK_UPDATE_CUSTOMER_ALLOCATION完了後
   処理: リバランスアラート生成
   
5. TASK_PREPARE_ALERT_EMAIL (末端Task)
   依存: TASK_GENERATE_REBALANCE_ALERTS完了後
   処理: 営業担当者向けメール準備

全体の処理フロー:
市場データロード → ポートフォリオ評価 → アロケーション分析 → アラート生成 → メール準備
*/