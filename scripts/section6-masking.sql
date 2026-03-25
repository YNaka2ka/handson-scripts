-- ============================================
-- Section 6: ダイナミックデータマスキング入門
-- ============================================
/*
このセクションで学ぶこと:
  1. マスキングポリシーの基本: ロールに応じてデータを動的にマスク
  2. 部分マスキング: メールアドレスや電話番号の一部だけを隠す
  3. 条件付きマスキング: 別カラムの値でマスク条件を変える
  4. マスキングの確認: ロールを切り替えて動作を検証

前提: Enterprise Edition 以上が必要
*/

USE ROLE ACCOUNTADMIN;
USE WAREHOUSE MUWH1;
USE SCHEMA SECURITIES_WORKSHOP.RAW_DATA;

-- ============================================
-- 1. 準備: マスキング対象のテスト用テーブル
-- ============================================

CREATE OR REPLACE TABLE SECURITIES_WORKSHOP.STAGING.CUSTOMER_CONTACTS (
  customer_id VARCHAR(10),
  customer_name VARCHAR(100),
  email VARCHAR(200),
  phone VARCHAR(20),
  risk_tolerance VARCHAR(20),
  visibility VARCHAR(10) DEFAULT 'PRIVATE'
);

INSERT INTO SECURITIES_WORKSHOP.STAGING.CUSTOMER_CONTACTS VALUES
  ('C001', '山田太郎', 'taro.yamada@example.com', '090-1234-5678', 'バランス', 'PUBLIC'),
  ('C002', '佐藤花子', 'hanako.sato@example.com', '080-2345-6789', '保守的', 'PRIVATE'),
  ('C003', '鈴木一郎', 'ichiro.suzuki@example.com', '070-3456-7890', '積極的', 'PRIVATE'),
  ('C004', '田中美咲', 'misaki.tanaka@example.com', '090-4567-8901', 'バランス', 'PUBLIC'),
  ('C005', '高橋健太', 'kenta.takahashi@example.com', '080-5678-9012', '保守的', 'PRIVATE');

SELECT * FROM SECURITIES_WORKSHOP.STAGING.CUSTOMER_CONTACTS;

-- ============================================
-- 2. 基本的なマスキングポリシー（フルマスク）
-- ============================================

-- ポリシー作成: ACCOUNTADMIN は見える、それ以外は '********'
CREATE OR REPLACE MASKING POLICY SECURITIES_WORKSHOP.STAGING.MASK_FULL_STRING
  AS (val STRING)
  RETURNS STRING ->
  CASE
    WHEN CURRENT_ROLE() IN ('ACCOUNTADMIN', 'SYSADMIN') THEN val
    ELSE '********'
  END;

-- メールカラムにポリシーを適用
ALTER TABLE SECURITIES_WORKSHOP.STAGING.CUSTOMER_CONTACTS
  MODIFY COLUMN email SET MASKING POLICY SECURITIES_WORKSHOP.STAGING.MASK_FULL_STRING;

-- ACCOUNTADMINで確認 → メールが見える
USE ROLE ACCOUNTADMIN;
SELECT customer_id, customer_name, email FROM SECURITIES_WORKSHOP.STAGING.CUSTOMER_CONTACTS;

-- MUロールで確認 → メールが '********' になる
USE ROLE MU;
SELECT customer_id, customer_name, email FROM SECURITIES_WORKSHOP.STAGING.CUSTOMER_CONTACTS;

-- ACCOUNTADMINに戻す
USE ROLE ACCOUNTADMIN;

-- ============================================
-- 3. 部分マスキング（メールのドメインだけ表示）
-- ============================================

-- 先にフルマスクを解除
ALTER TABLE SECURITIES_WORKSHOP.STAGING.CUSTOMER_CONTACTS
  MODIFY COLUMN email UNSET MASKING POLICY;

-- 部分マスキングポリシー: ドメイン部分だけ残す
CREATE OR REPLACE MASKING POLICY SECURITIES_WORKSHOP.STAGING.MASK_EMAIL_PARTIAL
  AS (val STRING)
  RETURNS STRING ->
  CASE
    WHEN CURRENT_ROLE() IN ('ACCOUNTADMIN', 'SYSADMIN') THEN val
    ELSE REGEXP_REPLACE(val, '.+\\@', '*****@')
  END;

-- メールカラムに適用
ALTER TABLE SECURITIES_WORKSHOP.STAGING.CUSTOMER_CONTACTS
  MODIFY COLUMN email SET MASKING POLICY SECURITIES_WORKSHOP.STAGING.MASK_EMAIL_PARTIAL;

-- MUロールで確認 → '*****@example.com' のように表示される
USE ROLE MU;
SELECT customer_id, customer_name, email FROM SECURITIES_WORKSHOP.STAGING.CUSTOMER_CONTACTS;

USE ROLE ACCOUNTADMIN;

-- ============================================
-- 4. 電話番号の部分マスキング（末尾4桁だけ表示）
-- ============================================

CREATE OR REPLACE MASKING POLICY SECURITIES_WORKSHOP.STAGING.MASK_PHONE
  AS (val STRING)
  RETURNS STRING ->
  CASE
    WHEN CURRENT_ROLE() IN ('ACCOUNTADMIN', 'SYSADMIN') THEN val
    ELSE CONCAT('***-****-', RIGHT(val, 4))
  END;

ALTER TABLE SECURITIES_WORKSHOP.STAGING.CUSTOMER_CONTACTS
  MODIFY COLUMN phone SET MASKING POLICY SECURITIES_WORKSHOP.STAGING.MASK_PHONE;

-- MUロールで確認 → '***-****-5678' のように表示される
USE ROLE MU;
SELECT customer_id, customer_name, phone FROM SECURITIES_WORKSHOP.STAGING.CUSTOMER_CONTACTS;

USE ROLE ACCOUNTADMIN;

-- ============================================
-- 5. 条件付きマスキング（visibilityカラムで制御）
-- ============================================

-- メールの部分マスクを解除
ALTER TABLE SECURITIES_WORKSHOP.STAGING.CUSTOMER_CONTACTS
  MODIFY COLUMN email UNSET MASKING POLICY;

-- 条件付きポリシー: visibility が PUBLIC なら誰でも見える
CREATE OR REPLACE MASKING POLICY SECURITIES_WORKSHOP.STAGING.MASK_EMAIL_CONDITIONAL
  AS (val STRING, visibility STRING)
  RETURNS STRING ->
  CASE
    WHEN CURRENT_ROLE() IN ('ACCOUNTADMIN', 'SYSADMIN') THEN val
    WHEN visibility = 'PUBLIC' THEN val
    ELSE REGEXP_REPLACE(val, '.+\\@', '*****@')
  END;

-- 条件付きポリシーの適用（USING句で条件カラムを指定）
ALTER TABLE SECURITIES_WORKSHOP.STAGING.CUSTOMER_CONTACTS
  MODIFY COLUMN email
  SET MASKING POLICY SECURITIES_WORKSHOP.STAGING.MASK_EMAIL_CONDITIONAL
  USING (email, visibility);

-- MUロールで確認
-- → visibility=PUBLIC の C001, C004 はメールが見える
-- → visibility=PRIVATE の C002, C003, C005 はマスクされる
USE ROLE MU;
SELECT customer_id, customer_name, email, visibility
FROM SECURITIES_WORKSHOP.STAGING.CUSTOMER_CONTACTS;

USE ROLE ACCOUNTADMIN;

-- ============================================
-- 6. マスキングポリシーの管理
-- ============================================

-- 現在のマスキングポリシー一覧
SHOW MASKING POLICIES IN SCHEMA SECURITIES_WORKSHOP.STAGING;

-- ポリシーの定義確認
DESCRIBE MASKING POLICY SECURITIES_WORKSHOP.STAGING.MASK_PHONE;

-- どのカラムにポリシーが適用されているか確認
SELECT *
FROM TABLE(INFORMATION_SCHEMA.POLICY_REFERENCES(
  POLICY_NAME => 'SECURITIES_WORKSHOP.STAGING.MASK_PHONE'
));

-- ============================================
-- 7. まとめ
-- ============================================

/*
┌──────────────────────┬─────────────────────────────────────────────┐
│ マスキング種類         │ 説明                                        │
├──────────────────────┼─────────────────────────────────────────────┤
│ フルマスク            │ 全体を '********' に置換                     │
│ 部分マスク            │ 一部だけ表示（ドメイン、末尾4桁など）           │
│ 条件付きマスク         │ 別カラムの値やロールに応じてマスク条件を変更     │
│ NULL マスク           │ 見せたくない場合は NULL を返す                 │
│ ハッシュマスク         │ SHA2 等でハッシュ化（集計には使える）           │
└──────────────────────┴─────────────────────────────────────────────┘

ポイント:
  - マスキングはクエリ実行時に動的に適用される（データ自体は変更されない）
  - 1つのポリシーを複数カラムに適用可能
  - ポリシーの入力型と出力型は一致させる必要がある
  - Enterprise Edition 以上が必要
*/

-- ============================================
-- 8. クリーンアップ（オプション）
-- ============================================

-- ポリシーを解除してからテーブル削除
-- ALTER TABLE SECURITIES_WORKSHOP.STAGING.CUSTOMER_CONTACTS MODIFY COLUMN email UNSET MASKING POLICY;
-- ALTER TABLE SECURITIES_WORKSHOP.STAGING.CUSTOMER_CONTACTS MODIFY COLUMN phone UNSET MASKING POLICY;
-- DROP TABLE IF EXISTS SECURITIES_WORKSHOP.STAGING.CUSTOMER_CONTACTS;
-- DROP MASKING POLICY IF EXISTS SECURITIES_WORKSHOP.STAGING.MASK_FULL_STRING;
-- DROP MASKING POLICY IF EXISTS SECURITIES_WORKSHOP.STAGING.MASK_EMAIL_PARTIAL;
-- DROP MASKING POLICY IF EXISTS SECURITIES_WORKSHOP.STAGING.MASK_PHONE;
-- DROP MASKING POLICY IF EXISTS SECURITIES_WORKSHOP.STAGING.MASK_EMAIL_CONDITIONAL;
