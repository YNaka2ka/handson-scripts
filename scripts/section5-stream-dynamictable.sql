-- ============================================
-- Section 5: ストリームとダイナミックテーブル入門
-- ============================================
/*
このセクションで学ぶこと:
  1. ストリーム: テーブルの変更（INSERT/UPDATE/DELETE）を追跡する仕組み
  2. ダイナミックテーブル: SELECTクエリから自動更新されるテーブル
  3. ストリーム+タスク vs ダイナミックテーブルの比較

前提: SECURITIES_WORKSHOP の既存テーブルを使用
*/

USE ROLE MU;
USE WAREHOUSE MUWH1;
USE SCHEMA SECURITIES_WORKSHOP.STAGING;

-- ============================================
-- 1. ストリームの基本
-- ============================================

-- HOLDINGS テーブルに対するストリームを作成
-- ストリームは「前回消費した時点」からの変更差分を記録する
CREATE OR REPLACE STREAM STREAM_HOLDINGS_CHANGES
  ON TABLE SECURITIES_WORKSHOP.RAW_DATA.HOLDINGS
  COMMENT = '保有銘柄テーブルの変更を追跡するストリーム';

-- ストリームの状態を確認（作成直後は空）
SELECT * FROM STREAM_HOLDINGS_CHANGES;
-- → 0件（まだ変更がないため）

-- ============================================
-- 2. ストリームでINSERTを検知する
-- ============================================

-- テスト用データを追加
INSERT INTO SECURITIES_WORKSHOP.RAW_DATA.HOLDINGS 
  (customer_id, stock_code, quantity, avg_purchase_price, purchase_date)
VALUES
  ('C001', '9999', 50, 3000.00, '2024-03-01');

-- ストリームを確認 → INSERT が記録されている
SELECT 
  CUSTOMER_ID,
  STOCK_CODE,
  QUANTITY,
  METADATA$ACTION,
  METADATA$ISUPDATE,
  METADATA$ROW_ID
FROM STREAM_HOLDINGS_CHANGES;

-- ポイント:
-- METADATA$ACTION = 'INSERT' → 新規追加された行
-- METADATA$ISUPDATE = FALSE → UPDATEではない（純粋なINSERT）

-- ============================================
-- 3. ストリームでUPDATEを検知する
-- ============================================

-- 先ほど追加した行を更新
UPDATE SECURITIES_WORKSHOP.RAW_DATA.HOLDINGS
SET quantity = 100
WHERE customer_id = 'C001' AND stock_code = '9999';

-- ストリームを確認 → INSERT後に消費していないため、ネットチェンジ（最終差分）のみ表示
SELECT 
  CUSTOMER_ID,
  STOCK_CODE,
  QUANTITY,
  METADATA$ACTION,
  METADATA$ISUPDATE
FROM STREAM_HOLDINGS_CHANGES;

-- ポイント:
-- ストリームは「前回消費時点からの最終的な差分」を返す
-- INSERT(50) → UPDATE(100) の中間状態は省略され、
-- 最終結果として INSERT(quantity=100) の1行だけが表示される
--
-- UPDATE を DELETE+INSERT のペアとして確認するには、
-- INSERT 後にストリームを一度消費（DMLで使用）してから UPDATE する必要がある

-- ============================================
-- 4. ストリームの消費（オフセットの前進）
-- ============================================

-- ストリームのデータをDMLで消費すると、オフセットが前進する
-- 変更履歴テーブルに記録する例

CREATE OR REPLACE TABLE SECURITIES_WORKSHOP.STAGING.HOLDINGS_CHANGE_LOG (
  change_timestamp TIMESTAMP_LTZ DEFAULT CURRENT_TIMESTAMP(),
  customer_id VARCHAR(10),
  stock_code VARCHAR(10),
  quantity NUMBER(10,0),
  action VARCHAR(10),
  is_update BOOLEAN
);

-- ストリームを消費（DML内で使うとオフセットが進む）
INSERT INTO SECURITIES_WORKSHOP.STAGING.HOLDINGS_CHANGE_LOG 
  (customer_id, stock_code, quantity, action, is_update)
SELECT 
  CUSTOMER_ID, STOCK_CODE, QUANTITY,
  METADATA$ACTION, METADATA$ISUPDATE
FROM STREAM_HOLDINGS_CHANGES;

-- ストリームは再び空になる（オフセットが最新に進んだ）
SELECT * FROM STREAM_HOLDINGS_CHANGES;
-- → 0件

-- 変更履歴テーブルには記録が残っている
SELECT * FROM SECURITIES_WORKSHOP.STAGING.HOLDINGS_CHANGE_LOG;

-- ============================================
-- 5. テストデータのクリーンアップ
-- ============================================

DELETE FROM SECURITIES_WORKSHOP.RAW_DATA.HOLDINGS
WHERE customer_id = 'C001' AND stock_code = '9999';

-- DELETEもストリームで検知される
SELECT 
  CUSTOMER_ID, STOCK_CODE, QUANTITY,
  METADATA$ACTION, METADATA$ISUPDATE
FROM STREAM_HOLDINGS_CHANGES;

-- ============================================
-- 6. ダイナミックテーブルの基本
-- ============================================

/*
ダイナミックテーブル vs ストリーム+タスク:

  ストリーム+タスク（命令型）:
    - 「どうやって変換するか」をコードで記述
    - MERGE, INSERT などのDMLを自分で書く
    - スケジュールも自分で管理

  ダイナミックテーブル（宣言型）:
    - 「どんな結果が欲しいか」をSELECTで記述
    - 更新の仕組みはSnowflakeが自動管理
    - TARGET_LAG で鮮度だけ指定すればOK
*/

-- ダイナミックテーブル: 顧客別ポートフォリオサマリー
-- TARGET_LAG = データの鮮度目標（元テーブルの変更からこの時間以内に反映）
CREATE OR REPLACE DYNAMIC TABLE SECURITIES_WORKSHOP.ANALYTICS.DT_CUSTOMER_PORTFOLIO_SUMMARY
  TARGET_LAG = '5 minutes'
  WAREHOUSE = MUWH1
AS
  SELECT 
    c.customer_id,
    c.customer_name,
    c.risk_tolerance,
    COUNT(h.stock_code) AS stock_count,
    SUM(h.quantity * h.avg_purchase_price) AS total_investment,
    SUM(h.quantity * mp.closing_price) AS total_current_value,
    ROUND(
      (SUM(h.quantity * mp.closing_price) - SUM(h.quantity * h.avg_purchase_price)) 
      / SUM(h.quantity * h.avg_purchase_price) * 100, 2
    ) AS overall_return_pct
  FROM SECURITIES_WORKSHOP.RAW_DATA.CUSTOMERS c
  JOIN SECURITIES_WORKSHOP.RAW_DATA.HOLDINGS h 
    ON c.customer_id = h.customer_id
  JOIN SECURITIES_WORKSHOP.ANALYTICS.V_LATEST_PRICES mp 
    ON h.stock_code = mp.stock_code
  GROUP BY c.customer_id, c.customer_name, c.risk_tolerance;

--顧客ごとに「いくら投資して、今いくらになっていて、何%の損益か」を算出する
--元データが変わるたびにこの集計が自動計算される

-- ダイナミックテーブルの状態確認
SHOW DYNAMIC TABLES LIKE 'DT_CUSTOMER_PORTFOLIO_SUMMARY' IN SCHEMA 

SECURITIES_WORKSHOP.ANALYTICS;

-- データを確認（自動的に最新データが反映されている）
SELECT * FROM SECURITIES_WORKSHOP.ANALYTICS.DT_CUSTOMER_PORTFOLIO_SUMMARY
ORDER BY customer_id;

-- ============================================
-- 7. ダイナミックテーブルの連鎖（パイプライン）
-- ============================================

-- ダイナミックテーブルは別のダイナミックテーブルを参照できる
-- → 多段のデータパイプラインを宣言的に構築可能

CREATE OR REPLACE DYNAMIC TABLE SECURITIES_WORKSHOP.ANALYTICS.DT_RISK_SEGMENT_SUMMARY
  TARGET_LAG = '10 minutes'
  WAREHOUSE = MUWH1
AS
  SELECT 
    risk_tolerance,
    COUNT(*) AS customer_count,
    ROUND(AVG(overall_return_pct), 2) AS avg_return_pct,
    SUM(total_investment) AS total_investment,
    SUM(total_current_value) AS total_current_value
  FROM SECURITIES_WORKSHOP.ANALYTICS.DT_CUSTOMER_PORTFOLIO_SUMMARY
  GROUP BY risk_tolerance;

-- リスクセグメント別サマリーを確認
SELECT * FROM SECURITIES_WORKSHOP.ANALYTICS.DT_RISK_SEGMENT_SUMMARY
ORDER BY risk_tolerance;

-- ============================================
-- 8. 比較: ストリーム+タスク vs ダイナミックテーブル
-- ============================================

/*
┌──────────────────────┬─────────────────────┬────────────────────────┐
│ 比較項目              │ ストリーム+タスク     │ ダイナミックテーブル      │
├──────────────────────┼─────────────────────┼────────────────────────┤
│ アプローチ            │ 命令型（How）        │ 宣言型（What）          │
│ コード量              │ 多い（MERGE等を記述） │ 少ない（SELECTのみ）    │
│ スケジュール管理       │ 自分で設定           │ TARGET_LAGで自動        │
│ 柔軟性               │ 高い（SP呼出等可能）  │ SELECTに限定            │
│ 非決定性関数          │ 使用可能             │ 一部制限あり             │
│ ストアドプロシージャ    │ 呼び出し可能         │ 使用不可                │
│ 向いている用途         │ 複雑なETL処理        │ 集計・変換パイプライン    │
└──────────────────────┴─────────────────────┴────────────────────────┘

使い分けの指針:
  - シンプルな集計・変換 → ダイナミックテーブル
  - MERGE/UPDATE/DELETEが必要 → ストリーム+タスク
  - 外部API呼出やSP実行が必要 → ストリーム+タスク
*/

-- ============================================
-- 9. クリーンアップ（オプション）
-- ============================================

-- ストリームの削除
-- DROP STREAM IF EXISTS STREAM_HOLDINGS_CHANGES;

-- ダイナミックテーブルの削除
-- DROP DYNAMIC TABLE IF EXISTS SECURITIES_WORKSHOP.ANALYTICS.DT_RISK_SEGMENT_SUMMARY;
-- DROP DYNAMIC TABLE IF EXISTS SECURITIES_WORKSHOP.ANALYTICS.DT_CUSTOMER_PORTFOLIO_SUMMARY;

-- 変更履歴テーブルの削除
-- DROP TABLE IF EXISTS SECURITIES_WORKSHOP.STAGING.HOLDINGS_CHANGE_LOG;
