select 'このviewはメモ(View definition 参照)' as memo

/* --- 気が付いたことメモ　　---


■ dbt 共通
　modelとか、コメント入れると挙動が変わることあり
　　コメントの正しい入れ方　調べたほうがいいかも
↓ 
　yos_wk_11 に入れるとき 
 jinja ? 学習中  https://zenn.dev/dbt_tokyo/books/537de43829f3a0/viewer/jinja_modeling


・カレンダーについて
　　データが日付型なら、過去12か月の yyyymmカレンダーテーブルとかあると使いやすいかも 




■ schema 書く時の疑問点・注意点　

〇 schama.yml がエラーだと全てのsqlが実行できなくなるので注意！！

〇 ITEM_CDがNULLの場合の対応は？
　　psi_month_2309_10_11
　　・マスタview（YOS_ITEM_CD_LIST） に NULL用のコードを追加してそれと紐づける、とか

〇 （あまり使うことないかもだが）Bで始まるなどの指定は出来ないっぽい。。 
         # - accepted_values:
          #     values: ['B%', 'P%']  # このような指定はできない様  https://docs.getdbt.com/reference/resource-properties/data-tests



■参考資料

〇dbt
　・使い方　（dbtとは？ 等）
　　　https://zenn.dev/dbt_tokyo/books/537de43829f3a0/viewer/tutorial
　・About data tests property　記述方法とか
　　　https://docs.getdbt.com/reference/resource-properties/data-tests

　・入門
　　　https://book.st-hakky.com/data-platform/dbt-intro/

〇Snowflake
　・リファレンス 関数など
　　　https://docs.snowflake.com/ja/sql-reference/functions-numeric



■ その他
・ファイル名は、数値から始めるのダメみたい　（000_aaa とかでrun model できなかった）


-------------------------------------
　わからんメモ
-------------------------------------

?? set に 変数を入れる方法は？？
-- {% set ITEM_CDs = ["VP4586040000", "VP4596040000", "VP9120010000"] %}




?? 使い方わからん　→解決
select * 
from 
    it_test_db.test_schema.psi_month_2309_10_11
--   {{ ref('yos_wk_02') }}  #← この書き方が使えない。?? schema には登録済み
     → sql ファイルの保存位置 　※schema と同じレベル（かそれ以下かと）　

*/
