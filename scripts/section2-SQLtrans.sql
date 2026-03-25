/*
このセクションのポイント:

段階的な計算: 簡単な計算から複雑な分析へステップアップ
CTE活用: 複雑なクエリを読みやすく分解
MERGE文: UPSERT処理で日次更新を効率化
業務ロジック: リバランス判定（±5%超）などの実務的な基準
アラート生成: 営業担当者向けの具体的なアクションを自動生成
*/
use role mu;
-- ============================================
-- SQLデータ変換セクション: ポートフォリオ分析
-- ============================================

USE SCHEMA SECURITIES_WORKSHOP.ANALYTICS;

-- ============================================
-- 1. 時価評価額の計算（保有数量 × 現在株価）
-- ============================================

-- 最新株価を使用した時価評価額計算
SELECT 
  h.customer_id,
  c.customer_name,
  h.stock_code,
  mp.stock_name,
  mp.sector,
  h.quantity,
  h.avg_purchase_price,
  mp.closing_price AS current_price,
  -- 簿価（取得原価）
  h.quantity * h.avg_purchase_price AS book_value,
  -- 時価評価額
  h.quantity * mp.closing_price AS market_value,
  mp.price_date AS valuation_date
FROM SECURITIES_WORKSHOP.RAW_DATA.HOLDINGS h
JOIN SECURITIES_WORKSHOP.RAW_DATA.CUSTOMERS c 
  ON h.customer_id = c.customer_id
JOIN SECURITIES_WORKSHOP.ANALYTICS.V_LATEST_PRICES mp 
  ON h.stock_code = mp.stock_code
ORDER BY h.customer_id, market_value DESC;

-- 顧客別の時価評価額合計
SELECT 
  c.customer_id,
  c.customer_name,
  c.risk_tolerance,
  COUNT(DISTINCT h.stock_code) AS num_holdings,
  SUM(h.quantity * h.avg_purchase_price) AS total_book_value,
  SUM(h.quantity * mp.closing_price) AS total_market_value,
  ROUND(total_market_value, 0) AS total_market_value_rounded
FROM SECURITIES_WORKSHOP.RAW_DATA.CUSTOMERS c
JOIN SECURITIES_WORKSHOP.RAW_DATA.HOLDINGS h 
  ON c.customer_id = h.customer_id
JOIN SECURITIES_WORKSHOP.ANALYTICS.V_LATEST_PRICES mp 
  ON h.stock_code = mp.stock_code
GROUP BY c.customer_id, c.customer_name, c.risk_tolerance
ORDER BY total_market_value DESC;

-- ============================================
-- 2. 評価損益の算出
-- ============================================

-- 銘柄別の評価損益詳細
SELECT 
  h.customer_id,
  c.customer_name,
  h.stock_code,
  mp.stock_name,
  mp.sector,
  h.quantity,
  h.avg_purchase_price,
  mp.closing_price AS current_price,
  -- 簿価
  h.quantity * h.avg_purchase_price AS book_value,
  -- 時価評価額
  h.quantity * mp.closing_price AS market_value,
  -- 評価損益（金額）
  (h.quantity * mp.closing_price) - (h.quantity * h.avg_purchase_price) AS unrealized_pl,
  -- 評価損益率（%）
  ROUND(
    ((h.quantity * mp.closing_price) - (h.quantity * h.avg_purchase_price)) 
    / NULLIF(h.quantity * h.avg_purchase_price, 0) * 100, 
    2
  ) AS unrealized_pl_pct,
  -- 損益判定
  CASE 
    WHEN (h.quantity * mp.closing_price) > (h.quantity * h.avg_purchase_price) THEN '含み益'
    WHEN (h.quantity * mp.closing_price) < (h.quantity * h.avg_purchase_price) THEN '含み損'
    ELSE '±0'
  END AS pl_status,
  h.purchase_date,
  DATEDIFF('day', h.purchase_date, mp.price_date) AS holding_days
FROM SECURITIES_WORKSHOP.RAW_DATA.HOLDINGS h
JOIN SECURITIES_WORKSHOP.RAW_DATA.CUSTOMERS c 
  ON h.customer_id = c.customer_id
JOIN SECURITIES_WORKSHOP.ANALYTICS.V_LATEST_PRICES mp 
  ON h.stock_code = mp.stock_code
ORDER BY unrealized_pl DESC;

-- 顧客別の評価損益サマリー
SELECT 
  c.customer_id,
  c.customer_name,
  c.risk_tolerance,
  c.sales_rep,
  COUNT(DISTINCT h.stock_code) AS num_holdings,
  SUM(h.quantity * h.avg_purchase_price) AS total_book_value,
  SUM(h.quantity * mp.closing_price) AS total_market_value,
  -- 評価損益合計
  SUM((h.quantity * mp.closing_price) - (h.quantity * h.avg_purchase_price)) AS total_unrealized_pl,
  -- 評価損益率
  ROUND(
    SUM((h.quantity * mp.closing_price) - (h.quantity * h.avg_purchase_price)) 
    / NULLIF(SUM(h.quantity * h.avg_purchase_price), 0) * 100, 
    2
  ) AS total_return_pct,
  -- 含み益銘柄数
  COUNT(CASE WHEN (h.quantity * mp.closing_price) > (h.quantity * h.avg_purchase_price) THEN 1 END) AS profit_holdings,
  -- 含み損銘柄数
  COUNT(CASE WHEN (h.quantity * mp.closing_price) < (h.quantity * h.avg_purchase_price) THEN 1 END) AS loss_holdings
FROM SECURITIES_WORKSHOP.RAW_DATA.CUSTOMERS c
JOIN SECURITIES_WORKSHOP.RAW_DATA.HOLDINGS h 
  ON c.customer_id = h.customer_id
JOIN SECURITIES_WORKSHOP.ANALYTICS.V_LATEST_PRICES mp 
  ON h.stock_code = mp.stock_code
GROUP BY c.customer_id, c.customer_name, c.risk_tolerance, c.sales_rep
ORDER BY total_return_pct DESC;

-- ============================================
-- 3. 顧客別の業種別保有比率の計算
-- ============================================

-- 顧客別・業種別の保有状況
WITH customer_sector_holdings AS (
  SELECT 
    h.customer_id,
    c.customer_name,
    c.risk_tolerance,
    mp.sector,
    SUM(h.quantity * mp.closing_price) AS sector_market_value
  FROM SECURITIES_WORKSHOP.RAW_DATA.HOLDINGS h
  JOIN SECURITIES_WORKSHOP.RAW_DATA.CUSTOMERS c 
    ON h.customer_id = c.customer_id
  JOIN SECURITIES_WORKSHOP.ANALYTICS.V_LATEST_PRICES mp 
    ON h.stock_code = mp.stock_code
  GROUP BY h.customer_id, c.customer_name, c.risk_tolerance, mp.sector
),
customer_total AS (
  SELECT 
    customer_id,
    SUM(sector_market_value) AS total_market_value
  FROM customer_sector_holdings
  GROUP BY customer_id
)
SELECT 
  csh.customer_id,
  csh.customer_name,
  csh.risk_tolerance,
  csh.sector,
  csh.sector_market_value,
  ct.total_market_value,
  -- 業種別配分比率（%）
  ROUND(csh.sector_market_value / ct.total_market_value * 100, 2) AS allocation_ratio
FROM customer_sector_holdings csh
JOIN customer_total ct 
  ON csh.customer_id = ct.customer_id
ORDER BY csh.customer_id, allocation_ratio DESC;

-- 業種別配分の可視化（ピボット形式）
SELECT 
  c.customer_id,
  c.customer_name,
  c.risk_tolerance,
  ROUND(SUM(CASE WHEN mp.sector = '自動車' THEN h.quantity * mp.closing_price ELSE 0 END) 
    / SUM(h.quantity * mp.closing_price) * 100, 2) AS "自動車_pct",
  ROUND(SUM(CASE WHEN mp.sector = '電機' THEN h.quantity * mp.closing_price ELSE 0 END) 
    / SUM(h.quantity * mp.closing_price) * 100, 2) AS "電機_pct",
  ROUND(SUM(CASE WHEN mp.sector = '銀行' THEN h.quantity * mp.closing_price ELSE 0 END) 
    / SUM(h.quantity * mp.closing_price) * 100, 2) AS "銀行_pct",
  ROUND(SUM(CASE WHEN mp.sector = '通信' THEN h.quantity * mp.closing_price ELSE 0 END) 
    / SUM(h.quantity * mp.closing_price) * 100, 2) AS "通信_pct",
  ROUND(SUM(CASE WHEN mp.sector = '医薬品' THEN h.quantity * mp.closing_price ELSE 0 END) 
    / SUM(h.quantity * mp.closing_price) * 100, 2) AS "医薬品_pct",
  ROUND(SUM(CASE WHEN mp.sector NOT IN ('自動車','電機','銀行','通信','医薬品') 
    THEN h.quantity * mp.closing_price ELSE 0 END) 
    / SUM(h.quantity * mp.closing_price) * 100, 2) AS "その他_pct",
  SUM(h.quantity * mp.closing_price) AS total_market_value
FROM SECURITIES_WORKSHOP.RAW_DATA.CUSTOMERS c
JOIN SECURITIES_WORKSHOP.RAW_DATA.HOLDINGS h 
  ON c.customer_id = h.customer_id
JOIN SECURITIES_WORKSHOP.ANALYTICS.V_LATEST_PRICES mp 
  ON h.stock_code = mp.stock_code
GROUP BY c.customer_id, c.customer_name, c.risk_tolerance
ORDER BY c.customer_id;

-- ============================================
-- 4. MERGEで日次ポートフォリオスナップショットテーブルを更新
-- ============================================

-- 本日分のスナップショットデータを作成してMERGE
MERGE INTO PORTFOLIO_SNAPSHOT ps
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

-- MERGE結果確認
SELECT 
  snapshot_date,
  COUNT(*) AS record_count,
  COUNT(DISTINCT customer_id) AS customer_count,
  SUM(market_value) AS total_market_value
FROM PORTFOLIO_SNAPSHOT
GROUP BY snapshot_date
ORDER BY snapshot_date DESC;

-- 本日のスナップショット詳細確認
SELECT * 
FROM PORTFOLIO_SNAPSHOT
WHERE snapshot_date = CURRENT_DATE()
ORDER BY customer_id, market_value DESC;

-- ============================================
-- 5. 顧客別アセットアロケーションテーブルの更新
-- ============================================

-- 業種別配分とターゲットの比較をMERGE
MERGE INTO CUSTOMER_ALLOCATION ca
USING (
  WITH sector_allocation AS (
    SELECT 
      ps.snapshot_date,
      ps.customer_id,
      c.risk_tolerance,
      ps.sector,
      SUM(ps.market_value) AS sector_market_value
    FROM PORTFOLIO_SNAPSHOT ps
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

-- MERGE結果確認
SELECT * 
FROM CUSTOMER_ALLOCATION
WHERE snapshot_date = CURRENT_DATE()
ORDER BY customer_id, ABS(deviation) DESC;

-- ============================================
-- 6. リスク許容度と実際のアロケーションの乖離分析
-- ============================================

-- 乖離が大きい顧客・業種の特定（±5%超）
SELECT 
  ca.customer_id,
  c.customer_name,
  ca.risk_tolerance,
  c.sales_rep,
  ca.sector,
  ca.allocation_ratio AS current_ratio,
  ca.target_ratio,
  ca.deviation,
  CASE 
    WHEN ca.deviation > 0 THEN 'オーバーウェイト'
    WHEN ca.deviation < 0 THEN 'アンダーウェイト'
    ELSE '適正'
  END AS allocation_status,
  ca.market_value AS sector_value
FROM CUSTOMER_ALLOCATION ca
JOIN SECURITIES_WORKSHOP.RAW_DATA.CUSTOMERS c 
  ON ca.customer_id = c.customer_id
WHERE ca.snapshot_date = CURRENT_DATE()
  AND ca.rebalance_flag = TRUE
ORDER BY ABS(ca.deviation) DESC;

-- 顧客別の乖離サマリー
SELECT 
  ca.customer_id,
  c.customer_name,
  ca.risk_tolerance,
  c.sales_rep,
  COUNT(*) AS total_sectors,
  SUM(CASE WHEN ca.rebalance_flag = TRUE THEN 1 ELSE 0 END) AS sectors_need_rebalance,
  ROUND(AVG(ABS(ca.deviation)), 2) AS avg_abs_deviation,
  ROUND(MAX(ABS(ca.deviation)), 2) AS max_abs_deviation,
  SUM(ca.market_value) AS total_portfolio_value
FROM CUSTOMER_ALLOCATION ca
JOIN SECURITIES_WORKSHOP.RAW_DATA.CUSTOMERS c 
  ON ca.customer_id = c.customer_id
WHERE ca.snapshot_date = CURRENT_DATE()
GROUP BY ca.customer_id, c.customer_name, ca.risk_tolerance, c.sales_rep
HAVING sectors_need_rebalance > 0
ORDER BY sectors_need_rebalance DESC, max_abs_deviation DESC;

-- リスク許容度別の乖離状況
SELECT 
  ca.risk_tolerance,
  COUNT(DISTINCT ca.customer_id) AS customer_count,
  ROUND(AVG(ABS(ca.deviation)), 2) AS avg_abs_deviation,
  COUNT(CASE WHEN ca.rebalance_flag = TRUE THEN 1 END) AS rebalance_needed_count,
  COUNT(*) AS total_allocations
FROM CUSTOMER_ALLOCATION ca
WHERE ca.snapshot_date = CURRENT_DATE()
GROUP BY ca.risk_tolerance
ORDER BY ca.risk_tolerance;

-- ヒートマップ用: 顧客×業種の乖離マトリクス
SELECT 
  ca.customer_id,
  MAX(CASE WHEN ca.sector = '自動車' THEN ca.deviation END) AS "自動車_deviation",
  MAX(CASE WHEN ca.sector = '電機' THEN ca.deviation END) AS "電機_deviation",
  MAX(CASE WHEN ca.sector = '銀行' THEN ca.deviation END) AS "銀行_deviation",
  MAX(CASE WHEN ca.sector = '通信' THEN ca.deviation END) AS "通信_deviation",
  MAX(CASE WHEN ca.sector = '医薬品' THEN ca.deviation END) AS "医薬品_deviation"
FROM CUSTOMER_ALLOCATION ca
WHERE ca.snapshot_date = CURRENT_DATE()
GROUP BY ca.customer_id
ORDER BY ca.customer_id;

-- ============================================
-- 7. リバランスアラートの生成
-- ============================================

-- リバランスが必要な顧客向けアラート作成
INSERT INTO REBALANCE_ALERTS (
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
FROM CUSTOMER_ALLOCATION ca
JOIN SECURITIES_WORKSHOP.RAW_DATA.CUSTOMERS c 
  ON ca.customer_id = c.customer_id
WHERE ca.snapshot_date = CURRENT_DATE()
  AND ca.rebalance_flag = TRUE
  AND NOT EXISTS (
    SELECT 1 
    FROM REBALANCE_ALERTS ra 
    WHERE ra.alert_date = CURRENT_DATE()
      AND ra.customer_id = ca.customer_id
      AND ra.sector = ca.sector
  );

-- 生成されたアラート確認
SELECT 
  alert_date,
  customer_id,
  customer_name,
  risk_tolerance,
  sales_rep,
  sector,
  current_ratio,
  target_ratio,
  deviation,
  recommended_action,
  alert_status
FROM REBALANCE_ALERTS
WHERE alert_date = CURRENT_DATE()
ORDER BY ABS(deviation) DESC;

-- 営業担当者別のアラート集計
SELECT 
  sales_rep,
  COUNT(DISTINCT customer_id) AS customers_need_action,
  COUNT(*) AS total_alerts,
  ROUND(AVG(ABS(deviation)), 2) AS avg_deviation
FROM REBALANCE_ALERTS
WHERE alert_date = CURRENT_DATE()
  AND alert_status = 'OPEN'
GROUP BY sales_rep
ORDER BY customers_need_action DESC;