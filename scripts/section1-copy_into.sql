/*
このセクションのポイント:

VALIDATION_MODE: 本番ロード前に検証可能
ON_ERROR: エラー時の挙動を制御（CONTINUE/ABORT_STATEMENT/SKIP_FILE）
エラーハンドリング: エラーレコードを別テーブルに退避
品質チェック: NULL、重複、参照整合性を体系的に検証
ログ管理: データ品質チェック結果を記録して追跡可能に
*/
use role mu;
-- ============================================
-- COPY INTOセクション: データロードと品質チェック
-- ============================================

USE SCHEMA SECURITIES_WORKSHOP.RAW_DATA;

-- ============================================
-- 1. ステージ上のファイル確認
-- ============================================

-- ステージ内のファイル一覧表示
LIST @WORKSHOP_STAGE;

-- ★★　どんなファイル形式のデータが入っているかな？ ★★
-- ステップ1: スキーマを推論　INFER_SCHEMA関数
SELECT * FROM TABLE(
  INFER_SCHEMA(
    LOCATION => '@WORKSHOP_STAGE/customers.csv',
    FILE_FORMAT => 'CSV_FORMAT'
  )
);
-- 中身が不明なファイルの確認には INFER_SCHEMA が便利です：

-- ステップ2:　各ファイルの中身をプレビュー（最初の10行）
SELECT $1, $2, $3, $4, $5
FROM @WORKSHOP_STAGE/customers.csv
(FILE_FORMAT => CSV_FORMAT)
LIMIT 10;

-- ============================================
-- 2. COPY INTO: 顧客マスタのロード　（検証モード：VALIDATION_MODE）
-- ============================================

-- 基本的なCOPY INTO
COPY INTO CUSTOMERS
FROM @WORKSHOP_STAGE/customers.csv
FILE_FORMAT = CSV_FORMAT
VALIDATION_MODE = 'RETURN_ERRORS';  -- ロードせずにエラー行のみ返す

--　「　クエリで結果が生成されませんでした　」　→エラーが発生しなかった
COPY INTO CUSTOMERS
FROM @WORKSHOP_STAGE/customers.csv
FILE_FORMAT = CSV_FORMAT
ON_ERROR = CONTINUE;  

-- ロード結果確認
SELECT * FROM TABLE(RESULT_SCAN(LAST_QUERY_ID()));

-- ロードされたデータ確認
SELECT * FROM CUSTOMERS;

-- ============================================
-- 3. COPY INTO: 市場株価データのロード　（カラムリスト、サブクエリ変換）
-- ============================================

-- ファイルの1行目だけを覗いてみる
COPY INTO market_prices
FROM @WORKSHOP_STAGE/market_prices.csv
FILE_FORMAT = CSV_FORMAT
VALIDATION_MODE = 'RETURN_1_ROWS';　　--1行目だけを表示する

-- ステップ1: 変換ロジックをSELECTで事前検証（ロードしない）
SELECT 	
  $1 AS stock_code,
  $2 AS stock_name,
  $3 AS sector,
  TO_DATE($4, 'YYYY-MM-DD') AS price_date,
  TO_NUMBER($5, 12, 2) AS closing_price,
  TO_NUMBER($6) AS volume,
  TO_NUMBER($7, 12, 2) AS week52_high,
  TO_NUMBER($8, 12, 2) AS week52_low
FROM @WORKSHOP_STAGE/market_prices.csv
(FILE_FORMAT => CSV_FORMAT)
LIMIT 10;

-- ステップ2: 問題なければ本ロード実行
COPY INTO MARKET_PRICES (
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
  FROM @WORKSHOP_STAGE/market_prices.csv
)
FILE_FORMAT = CSV_FORMAT
ON_ERROR = 'CONTINUE';

SELECT * FROM MARKET_PRICES;
-- ロード履歴確認
SELECT 
  table_name,
  file_name,
  row_count,
  row_parsed,
  error_count,
  first_error_message,
  status
FROM TABLE(INFORMATION_SCHEMA.COPY_HISTORY(
  TABLE_NAME => 'MARKET_PRICES',
  START_TIME => DATEADD(HOUR, -1, CURRENT_TIMESTAMP())
));

-- ============================================
-- 4. COPY INTO: 保有銘柄データのロード（エラーハンドリング）truncate table HOLDINGS;
-- ============================================
-- カラム数を気にせずステージ上のファイルの中身を確認するためのファイル形式を定義
CREATE FILE FORMAT IF NOT EXISTS CSV_RAW
  TYPE = 'CSV'
  FIELD_DELIMITER = 'NONE'
  SKIP_HEADER = 0;

SELECT $1 FROM @WORKSHOP_STAGE/holdings.csv
(FILE_FORMAT => 'CSV_RAW')
LIMIT 3;

-- ステップ1: ON_ERROR=CONTINUEでエラー行をスキップしてロード
COPY INTO HOLDINGS (
  customer_id,
  stock_code,
  quantity,
  avg_purchase_price,
  purchase_date
)
FROM (
  SELECT 
    $1,
    $2,
    TO_NUMBER($3),
    TO_NUMBER($4, 12, 2),
    TO_DATE($5, 'YYYY-MM-DD')
  FROM @WORKSHOP_STAGE/holdings.csv
)
FILE_FORMAT = CSV_FORMAT
ON_ERROR = 'CONTINUE'
RETURN_FAILED_ONLY = TRUE;   --複数ファイルを同時にロードする場合に、エラーのあったファイルだけ返す

-- 32行目がエラーであることを確認

-- エラーレコード確認 （エラーの行とその前の行を見比べてみる）
SELECT $1
FROM @WORKSHOP_STAGE/holdings.csv
(FILE_FORMAT => 'CSV_RAW')
LIMIT 2 OFFSET 30;

COPY INTO @WORKSHOP_STAGE/holdings_err1.csv
FROM (
  SELECT $1,$2,$3,$4,$5
  FROM @WORKSHOP_STAGE/holdings.csv
  (FILE_FORMAT => 'CSV_FORMAT')
  LIMIT 3 OFFSET 29
)
FILE_FORMAT = (TYPE = 'CSV' COMPRESSION = 'NONE')
SINGLE = TRUE
OVERWRITE = TRUE;

begin transaction;
INSERT INTO HOLDINGS (
  customer_id,
  stock_code,
  quantity,
  avg_purchase_price,
  purchase_date
)
SELECT 
  $1,
  0,
  0,
  0,
  '2024-01-01'
FROM @workshop_stage/holdings_err1.csv
(FILE_FORMAT => 'CSV_FORMAT')
;

-- ロードされたデータ確認
SELECT COUNT(*) AS loaded_count FROM HOLDINGS;
SELECT * FROM HOLDINGS ;
rollback;

-- ============================================
-- 5. COPY INTO: 残りのテーブルを一括ロード
-- ============================================

-- 日次取引データ
COPY INTO DAILY_TRADES (
  trade_date,
  customer_id,
  stock_code,
  trade_type,
  quantity,
  execution_price,
  commission
)
FROM (
  SELECT 
    TO_DATE($1, 'YYYY-MM-DD'),
    $2,
    $3,
    $4,
    TO_NUMBER($5),
    TO_NUMBER($6, 12, 2),
    TO_NUMBER($7, 12, 2)
  FROM @WORKSHOP_STAGE/daily_trades.csv
)
FILE_FORMAT = CSV_FORMAT
ON_ERROR = 'ABORT_STATEMENT';  -- エラーがあれば全体を中止

-- 目標アセットアロケーション
COPY INTO TARGET_ALLOCATION (
  risk_tolerance,
  sector,
  target_ratio
)
FROM (
  SELECT 
    $1,
    $2,
    TO_NUMBER($3, 5, 2)
  FROM @WORKSHOP_STAGE/target_allocation.csv
)
FILE_FORMAT = CSV_FORMAT
ON_ERROR = 'ABORT_STATEMENT';

-- ============================================
-- 6. データ品質チェック: NULL値検証
-- ============================================

-- 顧客マスタのNULLチェック
INSERT INTO SECURITIES_WORKSHOP.STAGING.DATA_QUALITY_LOG (
  table_name,
  check_type,
  check_result,
  affected_rows,
  details
)
SELECT 
  'CUSTOMERS' AS table_name,
  'NULL_CHECK' AS check_type,
  CASE WHEN COUNT(*) = 0 THEN 'PASS' ELSE 'FAIL' END AS check_result,
  COUNT(*) AS affected_rows,
  'NULL values found in: customer_id, customer_name, risk_tolerance, contract_date' AS details
FROM CUSTOMERS
WHERE customer_id IS NULL 
   OR customer_name IS NULL 
   OR risk_tolerance IS NULL 
   OR contract_date IS NULL;

-- 保有銘柄のNULLチェック
INSERT INTO SECURITIES_WORKSHOP.STAGING.DATA_QUALITY_LOG (
  table_name,
  check_type,
  check_result,
  affected_rows,
  details
)
SELECT 
  'HOLDINGS' AS table_name,
  'NULL_CHECK' AS check_type,
  CASE WHEN COUNT(*) = 0 THEN 'PASS' ELSE 'FAIL' END AS check_result,
  COUNT(*) AS affected_rows,
  'NULL values found in required columns' AS details
FROM HOLDINGS
WHERE customer_id IS NULL 
   OR stock_code IS NULL 
   OR quantity IS NULL 
   OR avg_purchase_price IS NULL;

-- 市場株価のNULLチェック
INSERT INTO SECURITIES_WORKSHOP.STAGING.DATA_QUALITY_LOG (
  table_name,
  check_type,
  check_result,
  affected_rows,
  details
)
SELECT 
  'MARKET_PRICES' AS table_name,
  'NULL_CHECK' AS check_type,
  CASE WHEN COUNT(*) = 0 THEN 'PASS' ELSE 'FAIL' END AS check_result,
  COUNT(*) AS affected_rows,
  'NULL values found in required columns' AS details
FROM MARKET_PRICES
WHERE stock_code IS NULL 
   OR closing_price IS NULL 
   OR price_date IS NULL;

-- 品質チェック結果確認
SELECT * FROM SECURITIES_WORKSHOP.STAGING.DATA_QUALITY_LOG
ORDER BY check_timestamp DESC;

-- ============================================
-- 7. データ品質チェック: 重複検証
-- ============================================

-- 顧客マスタの重複チェック（主キー重複）
INSERT INTO SECURITIES_WORKSHOP.STAGING.DATA_QUALITY_LOG (
  table_name,
  check_type,
  check_result,
  affected_rows,
  details
)
SELECT 
  'CUSTOMERS' AS table_name,
  'DUPLICATE_CHECK' AS check_type,
  CASE WHEN COUNT(*) = 0 THEN 'PASS' ELSE 'FAIL' END AS check_result,
  COUNT(*) AS affected_rows,
  'Duplicate customer_id found' AS details
FROM (
  SELECT customer_id, COUNT(*) AS cnt
  FROM CUSTOMERS
  GROUP BY customer_id
  HAVING COUNT(*) > 1
);

-- 保有銘柄の重複チェック（同一顧客・銘柄の重複保有）
INSERT INTO SECURITIES_WORKSHOP.STAGING.DATA_QUALITY_LOG (
  table_name,
  check_type,
  check_result,
  affected_rows,
  details
)
SELECT 
  'HOLDINGS' AS table_name,
  'DUPLICATE_CHECK' AS check_type,
  CASE WHEN COUNT(*) = 0 THEN 'PASS' ELSE 'FAIL' END AS check_result,
  COUNT(*) AS affected_rows,
  'Duplicate customer_id + stock_code found' AS details
FROM (
  SELECT customer_id, stock_code, COUNT(*) AS cnt
  FROM HOLDINGS
  GROUP BY customer_id, stock_code
  HAVING COUNT(*) > 1
);

-- 市場株価の重複チェック（同一銘柄・日付の重複）
INSERT INTO SECURITIES_WORKSHOP.STAGING.DATA_QUALITY_LOG (
  table_name,
  check_type,
  check_result,
  affected_rows,
  details
)
SELECT 
  'MARKET_PRICES' AS table_name,
  'DUPLICATE_CHECK' AS check_type,
  CASE WHEN COUNT(*) = 0 THEN 'PASS' ELSE 'FAIL' END AS check_result,
  COUNT(*) AS affected_rows,
  'Duplicate stock_code + price_date found' AS details
FROM (
  SELECT stock_code, price_date, COUNT(*) AS cnt
  FROM MARKET_PRICES
  GROUP BY stock_code, price_date
  HAVING COUNT(*) > 1
);

-- ============================================
-- 8. データ品質チェック: 参照整合性チェック
-- ============================================

-- 保有銘柄の顧客ID整合性チェック
INSERT INTO SECURITIES_WORKSHOP.STAGING.DATA_QUALITY_LOG (
  table_name,
  check_type,
  check_result,
  affected_rows,
  details
)
SELECT 
  'HOLDINGS' AS table_name,
  'REFERENTIAL_INTEGRITY' AS check_type,
  CASE WHEN COUNT(*) = 0 THEN 'PASS' ELSE 'FAIL' END AS check_result,
  COUNT(*) AS affected_rows,
  'Holdings with non-existent customer_id' AS details
FROM HOLDINGS h
LEFT JOIN CUSTOMERS c ON h.customer_id = c.customer_id
WHERE c.customer_id IS NULL;

-- 保有銘柄の銘柄コード整合性チェック
INSERT INTO SECURITIES_WORKSHOP.STAGING.DATA_QUALITY_LOG (
  table_name,
  check_type,
  check_result,
  affected_rows,
  details
)
SELECT 
  'HOLDINGS' AS table_name,
  'REFERENTIAL_INTEGRITY' AS check_type,
  CASE WHEN COUNT(*) = 0 THEN 'PASS' ELSE 'FAIL' END AS check_result,
  COUNT(*) AS affected_rows,
  'Holdings with non-existent stock_code in market prices' AS details
FROM (
  SELECT DISTINCT h.stock_code
  FROM HOLDINGS h
  LEFT JOIN MARKET_PRICES mp ON h.stock_code = mp.stock_code
  WHERE mp.stock_code IS NULL
);

-- ============================================
-- 9. 品質チェックサマリーレポート
-- ============================================

-- 全体サマリー
SELECT 
  table_name,
  check_type,
  check_result,
  affected_rows,
  check_timestamp
FROM SECURITIES_WORKSHOP.STAGING.DATA_QUALITY_LOG
ORDER BY check_timestamp DESC;

-- FAILしたチェックのみ表示
SELECT 
  table_name,
  check_type,
  affected_rows,
  details,
  check_timestamp
FROM SECURITIES_WORKSHOP.STAGING.DATA_QUALITY_LOG
WHERE check_result = 'FAIL'
ORDER BY check_timestamp DESC;

-- ============================================
-- 10. ロード統計サマリー
-- ============================================

-- テーブルごとのレコード数確認
SELECT 'CUSTOMERS' AS table_name, COUNT(*) AS row_count FROM CUSTOMERS
UNION ALL
SELECT 'HOLDINGS', COUNT(*) FROM HOLDINGS
UNION ALL
SELECT 'DAILY_TRADES', COUNT(*) FROM DAILY_TRADES
UNION ALL
SELECT 'MARKET_PRICES', COUNT(*) FROM MARKET_PRICES
UNION ALL
SELECT 'TARGET_ALLOCATION', COUNT(*) FROM TARGET_ALLOCATION;

