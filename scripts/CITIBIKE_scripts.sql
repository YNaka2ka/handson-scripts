/* *********************************************************************************** */
/* *** データベースとテーブルを作成する **************************************************** */
/* *********************************************************************************** */
-- 
-- 1.「CITIBIKE」という名前のデータベースを作成する
create database CITIBIKE comment ='Snowflakeハンズオン用';

-- 2.スキーマを作成する
-- 　　GUI操作（ナビゲーションメニュー　＞　カタログ　＞　データベースエクスプローラー　＞　データベース名）
--  スキーマ名「my_sc」

-- 3.テーブル「trips」を作成する
--   コンテキストを確認する

create or replace table trips  
(tripduration integer,
  starttime timestamp,
  stoptime timestamp,
  start_station_id integer,
  start_station_name string,
  start_station_latitude float,
  start_station_longitude float,
  end_station_id integer,
  end_station_name string,
  end_station_latitude float,
  end_station_longitude float,
  bikeid integer,
  membership_type string,
  usertype string,
  birth_year integer,
  gender integer);

-- 確認
show tables ;

-- 4.外部ステージを作る
--    GUI操作（ナビゲーションメニュー　＞　カタログ　＞　データベースエクスプローラー　＞　データベース名　＞　スキーマ名）
/* 
ステージ名：citibike_trips
URL：s3://snowflake-workshop-lab/japan/citibike-trips/
ディレクトリテーブルオプションをつける
   DIRECTORY = (ENABLE = TRUE)
*/

list @citibike_trips;
-- このフォルダにどんなファイルがあるか確認してみましょう

-- 5.ファイル形式を作成する

create or replace file format csv type='csv'
  compression = 'auto' field_delimiter = ',' record_delimiter = '\n'
  skip_header = 0 field_optionally_enclosed_by = '\042' trim_space = false
  error_on_column_count_mismatch = false escape = 'none' escape_unenclosed_field = '\134'
  date_format = 'auto' timestamp_format = 'auto' null_if = ('') comment = 'file format for ingesting data for zero to snowflake';

-- 確認
show file formats in database citibike;


/* *********************************************************************************** */
/* *** データのロード ******************************************************************* */
/* *********************************************************************************** */

-- 6.データロードのためのウェアハウスの作成
--    ウェアハウス名：my_wh
-- GUIでウェアハウスを作成（ナビゲーションメニュー　＞　コンピュート　＞　ウェアハウス）
-- または
-- コマンドで作成
    CREATE WAREHOUSE my_wh WITH WAREHOUSE_SIZE = 'XSMALL'
    AUTO_SUSPEND = 120 AUTO_RESUME = TRUE 
    INITIALLY_SUSPENDED = TRUE;   --作成直後は自動停止

    use warehouse my_wh;
    
-- 7.データのロード「COPY INTO」コマンド
-- ロードにどれくらいの時間がかかるでしょうか
copy into trips from @citibike_trips file_format=csv PATTERN = '.*csv.*' ;

-- 確認
-- tripsに入ったデータ件数は SELECTしなくてもわかります
-- Snowsightをリフレッシュして、テーブルをクリックしてみましょう

-- 8.テーブルのデータをいったん削除します

truncate table trips;

--確認
select * from trips limit 10;

-- 9.ウェアハウスのサイズを変更

alter warehouse my_wh set warehouse_size='medium'; 

-- 確認
show warehouses;

-- 10.さきほどと同じデータロード「COPY INTO」を実行

copy into trips from @citibike_trips file_format=csv PATTERN = '.*csv.*' ;
-- どのくらい速くなりましたか？

-- 確認
select * from trips limit 20;

-- ウェアハウスのサイズをSMALLにしておきましょう
-- なお、業務利用の場合、恒常的に大規模処理を行わないウェアハウスのサイズはX-SMALLにしておくことをお勧めします

alter warehouse my_wh set warehouse_size='small'; 

/* *********************************************************************************** */
/* *クエリ、結果キャッシュ、クローンの操作 ************************************************** */
/* *********************************************************************************** */

-- 11.Citi Bikeの利用状況に関する基本的な時間別統計をいくつか見ていきましょう。
-- 実行して、速度を確認しましょう

select date_trunc('hour', starttime) as "date",
count(*) as "num trips",
avg(tripduration)/60 as "avg duration (mins)", 
avg(haversine(start_station_latitude, start_station_longitude, end_station_latitude, end_station_longitude)) as "avg distance (km)" 
from trips
group by 1 order by 1;

-- こちらも実行して、速度を確認しましょう

select date_trunc('hour', starttime) as "date",
count(*) as "num trips",
avg(tripduration)/60 as "avg duration (mins)", 
avg(haversine(start_station_latitude, start_station_longitude, end_station_latitude, end_station_longitude)) as "avg distance (km)" 
from trips
group by 1 order by 1;

-- 結果キャッシュの使用

-- 12 以下の2つは別のSQLだが、キャッシュが使われていることをプロファイルで確認する

select
monthname(starttime) as "month",
count(*) as "num trips"
from trips
group by 1 order by 2 desc;

select
dayname(starttime) as "day of week",
count(*) as "num trips"
from trips
group by 1 order by 2 desc;

-- 13.テーブルの複製（ゼロコピークローン）

create table trips_dev clone trips;


/* *********************************************************************************** */
/* *** 半構造化データ、ビュー、結合の操作 *************************************************** */
/* *********************************************************************************** */

-- 14.別のデータベース「weather」を作成

create database weather;

-- 15.コンテキストの設定、確認
-- コマンドでコンテキストを変更してみましょう

use database weather;

use role accountadmin;
use warehouse my_wh; 
use database weather;

-- 16.自分のスキーマを作成

create schema my_sc;

use schema my_sc;

-- 17.JSONデータ用のテーブルを作成

create table json_weather_data (v variant);

-- 確認
show tables in database weather;

-- 18.別の外部ステージの作成

create stage nyc_weather
url = 's3://snowflake-workshop-lab/weather-nyc';

-- 19.ファイルを確認

list @nyc_weather;

-- 20.半構造化データのロード
-- ファイルフォーマットを事前に定義するのではなく、copy intoコマンドの中でファイルフォーマットを指定することもできます

copy into json_weather_data
from @nyc_weather 
file_format = (type = json strip_outer_array = true);

-- 21.半構造化データの検証

select * from json_weather_data limit 10;

-- Snowsight画面　どれかデータをクリックすると右側のパネルにフォーマットされたJSONが表示

-- 22. 半構造化JSON気象データを確認するためのビューを作成

create view json_weather_data_view as
select
  v:time::timestamp as observation_time,
  v:city.id::int as station_id,
  v:city.name::string as city_name,
  v:city.country::string as country,
  v:city.coord.lat::float as city_lat,
  v:city.coord.lon::float as city_lon,
  v:clouds.all::int as clouds,
  (v:main.temp::float)-273.15 as temp_avg,
  (v:main.temp_min::float)-273.15 as temp_min,
  (v:main.temp_max::float)-273.15 as temp_max,
  v:weather[0].main::string as weather,
  v:weather[0].description::string as weather_desc,
  v:weather[0].icon::string as weather_icon,
  v:wind.deg::float as wind_dir,
  v:wind.speed::float as wind_speed
from json_weather_data
where station_id = 5128638;

-- 23.ビューを確認

select * from json_weather_data_view
where date_trunc('month',observation_time) = '2018-01-01' 
limit 20;

-- JSON形式のデータに SQLでアクセスできました

-- 24.結合操作を使用してデータセットに対して相関させる

select weather as conditions
    ,count(*) as num_trips
from citibike.my_sc.trips 
left outer join json_weather_data_view
    on date_trunc('hour', observation_time) = date_trunc('hour', starttime)
where conditions is not null
group by 1 order by 2 desc;


/* *********************************************************************************** */
/* *** タイムトラベルの使用 ************************************************************** */
/* *********************************************************************************** */

-- 25.誤って、または意図的に削除されたデータオブジェクトを復元する方法を見てみましょう。

drop table json_weather_data;

-- 確認

select * from json_weather_data limit 10;

-- 26.テーブルを復元します。

undrop table json_weather_data;

-- 確認

select * from json_weather_data limit 10;

-- 27.テーブルのロールバック

-- コンテキストを切り替えます
use warehouse my_wh;
use database citibike;
use schema my_sc;

-- 28.データを更新します

update trips set start_station_name = 'oops';

-- 29.start_station_name毎の利用数を見てみましょう

select 
start_station_name as "station",
count(*) as "rides"
from trips
group by 1
order by 2 desc
limit 20;

-- opps!   何が起こりましたか？

-- 大変です。start_station_nameを誤って opps!に更新してしまいました！
-- でも大丈夫。データをさきほどupdateを実行したときの直前の状態に戻しましょう

-- 30 まず、Query IDを取り、変数"query_id"に代入します（SnowsightのGUIの方で確認してもよいです。）
set query_id = 
(select query_id from 
table(information_schema.query_history_by_session (result_limit=>5)) 
where query_text like 'update%' order by start_time limit 1);

-- 31.取得したQuery_IDのbeforeの状態のテーブルが見れます

select * from trips before (statement => $query_id) limit 3;

-- 32.その状態にテーブルを入れ替えましょう

create or replace table trips as
(select * from trips before (statement => $query_id));
        
-- 確認です。

select 
start_station_name as "station",
count(*) as "rides"
from trips
group by 1
order by 2 desc
limit 20;

-- start_station_name毎の利用数が取れましたか？


-- 次は、Streamlit in Snowflakeを用いて、データ分析用のアプリを作成して見ましょう！
-- 講師説明



-- 【オプション】これより先は作成したオブジェクトをすべて削除したい場合のみ実行してください
-- use role accountadmin;
-- use warehouse compute_wh;
-- use database weather;
-- use schema my_sc;

-- drop share if exists trips_share;
-- drop database if exists citibike;
-- drop database if exists weather;
-- drop warehouse if exists analytics_wh;
-- drop role if exists junior_dba;

