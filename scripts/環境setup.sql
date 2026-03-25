-- ============================================
-- 1. 環境セットアップ
-- ============================================
use role securityadmin;
create role mu;
grant role mu to role sysadmin;
use role sysadmin;
CREATE WAREHOUSE MUWH1
  WAREHOUSE_SIZE = 'XSMALL'
  AUTO_SUSPEND = 60
  AUTO_RESUME = TRUE
  INITIALLY_SUSPENDED = TRUE; 
grant ownership on warehouse muwh1 to role mu;
grant role mu to user user1;

-- データベース作成
CREATE OR REPLACE DATABASE SECURITIES_WORKSHOP
  COMMENT = '証券会社データエンジニアリングワークショップ用データベース';

-- スキーマ作成
CREATE OR REPLACE SCHEMA SECURITIES_WORKSHOP.RAW_DATA
  COMMENT = '生データ格納スキーマ（COPY INTOのロード先）';

CREATE OR REPLACE SCHEMA SECURITIES_WORKSHOP.STAGING
  COMMENT = 'ステージングデータとデータ品質管理用スキーマ';

CREATE OR REPLACE SCHEMA SECURITIES_WORKSHOP.ANALYTICS
  COMMENT = '分析用加工データ格納スキーマ';

grant usage on database SECURITIES_WORKSHOP to role mu;
grant ownership on schema RAW_DATA to mu;
grant ownership on schema STAGING to mu;
grant ownership on schema ANALYTICS to mu;

use role mu;
USE SCHEMA SECURITIES_WORKSHOP.RAW_DATA;

-- ============================================
-- 2. 内部ステージとファイル形式の作成
-- ============================================

-- 内部ステージ作成
CREATE OR REPLACE STAGE WORKSHOP_STAGE
  COMMENT = 'ワークショップ用サンプルデータファイル格納ステージ';

-- CSVファイル形式定義
CREATE OR REPLACE FILE FORMAT CSV_FORMAT
  TYPE = 'CSV'
  FIELD_DELIMITER = ','
  SKIP_HEADER = 1
  FIELD_OPTIONALLY_ENCLOSED_BY = '"'
  TRIM_SPACE = TRUE
  ERROR_ON_COLUMN_COUNT_MISMATCH = FALSE
  ENCODING = 'UTF8'
  COMMENT = 'UTF8エンコードCSVファイル用フォーマット定義';

-- ============================================
-- 3. RAWデータテーブル（COPY INTO先）
-- ============================================

-- 顧客マスタテーブル
CREATE OR REPLACE TABLE CUSTOMERS (
  customer_id VARCHAR(10) PRIMARY KEY COMMENT '顧客ID（主キー）',
  customer_name VARCHAR(100) NOT NULL COMMENT '顧客名',
  risk_tolerance VARCHAR(20) NOT NULL COMMENT 'リスク許容度（保守的/バランス/積極的）',
  contract_date DATE NOT NULL COMMENT '契約日',
  sales_rep VARCHAR(100) COMMENT '担当営業担当者名',
  load_timestamp TIMESTAMP_LTZ DEFAULT CURRENT_TIMESTAMP() COMMENT 'データロードタイムスタンプ'
) COMMENT = '顧客マスタテーブル - 顧客基本情報と投資プロファイルを管理';

-- 保有銘柄テーブル
CREATE OR REPLACE TABLE HOLDINGS (
  holding_id NUMBER AUTOINCREMENT PRIMARY KEY COMMENT '保有ID（自動採番主キー）',
  customer_id VARCHAR(10) NOT NULL COMMENT '顧客ID',
  stock_code VARCHAR(10) NOT NULL COMMENT '銘柄コード（4桁証券コード）',
  quantity NUMBER(10,0) NOT NULL COMMENT '保有数量（株数）',
  avg_purchase_price NUMBER(12,2) NOT NULL COMMENT '平均取得単価（円）',
  purchase_date DATE NOT NULL COMMENT '取得日',
  load_timestamp TIMESTAMP_LTZ DEFAULT CURRENT_TIMESTAMP() COMMENT 'データロードタイムスタンプ',
  CONSTRAINT fk_holdings_customer FOREIGN KEY (customer_id) REFERENCES CUSTOMERS(customer_id)
) COMMENT = '顧客保有銘柄テーブル - 顧客ごとの株式保有状況を管理';

-- 日次取引データテーブル
CREATE OR REPLACE TABLE DAILY_TRADES (
  trade_id NUMBER AUTOINCREMENT PRIMARY KEY COMMENT '取引ID（自動採番主キー）',
  trade_date DATE NOT NULL COMMENT '取引日（約定日）',
  customer_id VARCHAR(10) NOT NULL COMMENT '顧客ID',
  stock_code VARCHAR(10) NOT NULL COMMENT '銘柄コード',
  trade_type VARCHAR(10) NOT NULL COMMENT '売買区分（買/売）',
  quantity NUMBER(10,0) NOT NULL COMMENT '取引数量（株数）',
  execution_price NUMBER(12,2) NOT NULL COMMENT '約定単価（円）',
  commission NUMBER(12,2) NOT NULL COMMENT '取引手数料（円）',
  load_timestamp TIMESTAMP_LTZ DEFAULT CURRENT_TIMESTAMP() COMMENT 'データロードタイムスタンプ',
  CONSTRAINT fk_trades_customer FOREIGN KEY (customer_id) REFERENCES CUSTOMERS(customer_id)
) COMMENT = '日次取引データテーブル - 顧客の売買取引履歴を管理';

-- 市場株価データテーブル
CREATE OR REPLACE TABLE MARKET_PRICES (
  price_id NUMBER AUTOINCREMENT PRIMARY KEY COMMENT '価格ID（自動採番主キー）',
  stock_code VARCHAR(10) NOT NULL COMMENT '銘柄コード',
  stock_name VARCHAR(100) NOT NULL COMMENT '銘柄名',
  sector VARCHAR(50) NOT NULL COMMENT '業種分類',
  price_date DATE NOT NULL COMMENT '株価日付',
  closing_price NUMBER(12,2) NOT NULL COMMENT '終値（円）',
  volume NUMBER(15,0) COMMENT '出来高（株数）',
  week52_high NUMBER(12,2) COMMENT '52週高値（円）',
  week52_low NUMBER(12,2) COMMENT '52週安値（円）',
  load_timestamp TIMESTAMP_LTZ DEFAULT CURRENT_TIMESTAMP() COMMENT 'データロードタイムスタンプ',
  CONSTRAINT uk_market_prices UNIQUE (stock_code, price_date)
) COMMENT = '市場株価データテーブル - 日次の株価情報を管理';

-- 目標アセットアロケーションテーブル
CREATE OR REPLACE TABLE TARGET_ALLOCATION (
  allocation_id NUMBER AUTOINCREMENT PRIMARY KEY COMMENT 'アロケーションID（自動採番主キー）',
  risk_tolerance VARCHAR(20) NOT NULL COMMENT 'リスク許容度',
  sector VARCHAR(50) NOT NULL COMMENT '業種',
  target_ratio NUMBER(5,2) NOT NULL COMMENT '目標配分比率（%）',
  load_timestamp TIMESTAMP_LTZ DEFAULT CURRENT_TIMESTAMP() COMMENT 'データロードタイムスタンプ',
  CONSTRAINT uk_target_allocation UNIQUE (risk_tolerance, sector)
) COMMENT = '目標アセットアロケーションテーブル - リスク許容度別の推奨業種配分を管理';

-- ============================================
-- 4. STAGINGテーブル（データ品質チェック用）
-- ============================================

USE SCHEMA SECURITIES_WORKSHOP.STAGING;

-- エラーレコード格納テーブル
CREATE OR REPLACE TABLE LOAD_ERRORS (
  error_id NUMBER AUTOINCREMENT PRIMARY KEY COMMENT 'エラーID（自動採番主キー）',
  table_name VARCHAR(100) COMMENT 'ロード対象テーブル名',
  file_name VARCHAR(500) COMMENT 'ロード元ファイル名',
  error_message VARCHAR(5000) COMMENT 'エラーメッセージ詳細',
  rejected_record VARCHAR(1000) COMMENT '拒否されたレコード',
  error_timestamp TIMESTAMP_LTZ DEFAULT CURRENT_TIMESTAMP() COMMENT 'エラー発生タイムスタンプ'
) COMMENT = 'データロードエラー記録テーブル - COPY INTOで失敗したレコードを保存';

-- データ品質チェック結果テーブル
CREATE OR REPLACE TABLE DATA_QUALITY_LOG (
  log_id NUMBER AUTOINCREMENT PRIMARY KEY COMMENT 'ログID（自動採番主キー）',
  table_name VARCHAR(100) COMMENT 'チェック対象テーブル名',
  check_type VARCHAR(100) COMMENT 'チェック種別（NULL検証、重複チェック等）',
  check_result VARCHAR(20) COMMENT 'チェック結果（PASS/FAIL）',
  affected_rows NUMBER COMMENT '影響を受けた行数',
  check_timestamp TIMESTAMP_LTZ DEFAULT CURRENT_TIMESTAMP() COMMENT 'チェック実行タイムスタンプ',
  details VARCHAR(5000) COMMENT 'チェック詳細情報'
) COMMENT = 'データ品質チェックログテーブル - データ品質検証結果を記録';

-- ============================================
-- 5. ANALYTICSテーブル（分析用）
-- ============================================

USE SCHEMA SECURITIES_WORKSHOP.ANALYTICS;

-- 日次ポートフォリオスナップショット
CREATE OR REPLACE TABLE PORTFOLIO_SNAPSHOT (
  snapshot_id NUMBER AUTOINCREMENT PRIMARY KEY COMMENT 'スナップショットID（自動採番主キー）',
  snapshot_date DATE NOT NULL COMMENT 'スナップショット日付',
  customer_id VARCHAR(10) NOT NULL COMMENT '顧客ID',
  stock_code VARCHAR(10) NOT NULL COMMENT '銘柄コード',
  stock_name VARCHAR(100) COMMENT '銘柄名',
  sector VARCHAR(50) COMMENT '業種',
  quantity NUMBER(10,0) COMMENT '保有数量',
  avg_purchase_price NUMBER(12,2) COMMENT '平均取得単価（円）',
  current_price NUMBER(12,2) COMMENT '現在株価（円）',
  market_value NUMBER(15,2) COMMENT '時価評価額（円）= 数量 × 現在株価',
  unrealized_pl NUMBER(15,2) COMMENT '評価損益（円）= 時価評価額 - 簿価',
  unrealized_pl_pct NUMBER(8,4) COMMENT '評価損益率（%）',
  load_timestamp TIMESTAMP_LTZ DEFAULT CURRENT_TIMESTAMP() COMMENT 'データロードタイムスタンプ',
  CONSTRAINT uk_portfolio_snapshot UNIQUE (snapshot_date, customer_id, stock_code)
) COMMENT = '日次ポートフォリオスナップショットテーブル - 日次で顧客ポートフォリオの時価評価を記録';

-- 顧客別アセットアロケーション
CREATE OR REPLACE TABLE CUSTOMER_ALLOCATION (
  allocation_id NUMBER AUTOINCREMENT PRIMARY KEY COMMENT 'アロケーションID（自動採番主キー）',
  snapshot_date DATE NOT NULL COMMENT 'スナップショット日付',
  customer_id VARCHAR(10) NOT NULL COMMENT '顧客ID',
  risk_tolerance VARCHAR(20) COMMENT 'リスク許容度',
  sector VARCHAR(50) COMMENT '業種',
  market_value NUMBER(15,2) COMMENT '業種別時価評価額（円）',
  allocation_ratio NUMBER(5,2) COMMENT '実際の配分比率（%）',
  target_ratio NUMBER(5,2) COMMENT '目標配分比率（%）',
  deviation NUMBER(5,2) COMMENT '乖離（%）= 実際の配分比率 - 目標配分比率',
  rebalance_flag BOOLEAN COMMENT 'リバランス要否フラグ（乖離±5%超でTRUE）',
  load_timestamp TIMESTAMP_LTZ DEFAULT CURRENT_TIMESTAMP() COMMENT 'データロードタイムスタンプ',
  CONSTRAINT uk_customer_allocation UNIQUE (snapshot_date, customer_id, sector)
) COMMENT = '顧客別アセットアロケーションテーブル - 顧客ごとの業種別配分状況と目標との乖離を管理';

-- リバランスアラート
CREATE OR REPLACE TABLE REBALANCE_ALERTS (
  alert_id NUMBER AUTOINCREMENT PRIMARY KEY COMMENT 'アラートID（自動採番主キー）',
  alert_date DATE NOT NULL COMMENT 'アラート発生日',
  customer_id VARCHAR(10) NOT NULL COMMENT '顧客ID',
  customer_name VARCHAR(100) COMMENT '顧客名',
  risk_tolerance VARCHAR(20) COMMENT 'リスク許容度',
  sales_rep VARCHAR(100) COMMENT '担当営業担当者名',
  sector VARCHAR(50) COMMENT '対象業種',
  current_ratio NUMBER(5,2) COMMENT '現在の配分比率（%）',
  target_ratio NUMBER(5,2) COMMENT '目標配分比率（%）',
  deviation NUMBER(5,2) COMMENT '乖離（%）',
  recommended_action VARCHAR(500) COMMENT '推奨アクション内容',
  alert_status VARCHAR(20) DEFAULT 'OPEN' COMMENT 'アラートステータス（OPEN/CLOSED/IN_PROGRESS）',
  load_timestamp TIMESTAMP_LTZ DEFAULT CURRENT_TIMESTAMP() COMMENT 'アラート生成タイムスタンプ'
) COMMENT = 'リバランスアラートテーブル - 配分乖離が大きい顧客への営業アクション管理';

-- ============================================
-- 6. 便利なビュー
-- ============================================

-- 最新株価ビュー
CREATE OR REPLACE VIEW V_LATEST_PRICES
  COMMENT = '最新株価ビュー - 各銘柄の最新取引日の株価情報を表示'
AS
SELECT 
  stock_code,
  stock_name,
  sector,
  closing_price,
  volume,
  price_date,
  week52_high,
  week52_low,
  ROUND((closing_price - week52_low) / (week52_high - week52_low) * 100, 2) AS price_position_pct
FROM SECURITIES_WORKSHOP.RAW_DATA.MARKET_PRICES
QUALIFY ROW_NUMBER() OVER (PARTITION BY stock_code ORDER BY price_date DESC) = 1;

-- 顧客ポートフォリオサマリービュー
CREATE OR REPLACE VIEW V_CUSTOMER_PORTFOLIO_SUMMARY
  COMMENT = '顧客ポートフォリオサマリービュー - 顧客ごとの保有状況と損益を集計'
AS
SELECT 
  c.customer_id,
  c.customer_name,
  c.risk_tolerance,
  c.sales_rep,
  COUNT(DISTINCT h.stock_code) AS num_holdings,
  SUM(h.quantity * mp.closing_price) AS total_market_value,
  SUM(h.quantity * h.avg_purchase_price) AS total_cost,
  SUM(h.quantity * mp.closing_price) - SUM(h.quantity * h.avg_purchase_price) AS total_unrealized_pl,
  ROUND((SUM(h.quantity * mp.closing_price) - SUM(h.quantity * h.avg_purchase_price)) 
        / NULLIF(SUM(h.quantity * h.avg_purchase_price), 0) * 100, 2) AS total_return_pct
FROM SECURITIES_WORKSHOP.RAW_DATA.CUSTOMERS c
JOIN SECURITIES_WORKSHOP.RAW_DATA.HOLDINGS h ON c.customer_id = h.customer_id
JOIN V_LATEST_PRICES mp ON h.stock_code = mp.stock_code
GROUP BY c.customer_id, c.customer_name, c.risk_tolerance, c.sales_rep;

-- ============================================
-- 7. カラムコメント確認用クエリ
-- ============================================

-- 特定テーブルのカラムコメント確認
-- DESCRIBE TABLE SECURITIES_WORKSHOP.RAW_DATA.CUSTOMERS;
-- DESCRIBE TABLE SECURITIES_WORKSHOP.ANALYTICS.PORTFOLIO_SNAPSHOT;

-- テーブル一覧とコメント確認
-- SHOW TABLES IN DATABASE SECURITIES_WORKSHOP;

-- ステージ確認
-- LIST @SECURITIES_WORKSHOP.RAW_DATA.WORKSHOP_STAGE;


-- ============================================
-- 999. 環境削除
-- ============================================
-- use role sysadmin;
-- drop database securities_workshop;
drop warehouse muwh1;
drop role mu;
