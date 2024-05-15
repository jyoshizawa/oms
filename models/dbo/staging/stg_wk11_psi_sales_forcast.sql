

----------------------------------------------------------------------------
-- stg_wk11_psi_sales_forcast
-- 
-- stg_VIEW_PSISalesForcast　に Localのソート情報、DESCRIPTION を追加
--　※ ↑ は SQL Server の 「View_PSISalesForcast」を再現させたので そちらへの修正はせず ここで対応
-- Qlikでのパラメータ確認用に作成
----------------------------------------------------------------------------

-- Local 情報
with w_local_list as (
    select *
    from (
        values
        ( 11, 'S031',	'02_EU' ),
        ( 12, 'S041',	'03_US' ),
        ( 13, 'S001',	'01_JP' ),
        ( 14, 'S011',	'04_CN' ),
        ( 15, 'S051',	'05_AU' ),
        ( 21, 'S015',	'06_HK' ),
        ( 22, 'S019',	'12_SP' ),
        ( 23, 'S018',	'11_ML' ),
        ( 24, 'S020',	'13_TH' ),
        ( 25, 'S017',	'14_TW' ),
        ( 26, 'S021',	'15_IN' ),
        ( 31, 'S002',	'HQ Others' )
    ) as lical_list(SITE_SORT, SITE_CD, SITE_NAME )
                  --( ソート順、 コード、名称 )
) 


select
    t1.*,
    t3.SITE_NAME,
    t3.SITE_SORT,
    t4.ZZKAISO,
    t4.DESCRIPTION
from
-- IT_TEST_DB.DBT_4_YOSHI.STG_VIEW_PSISALESFORCAST
    {{ref('stg_VIEW_PSISalesForcast')}} as t1
left join 
    w_local_list as t3
on
    t1.SITE_CD = t3.SITE_CD

left join   
    {{ref('stg_View_ITEM_CLASSIFICATION_LIST')}} as t4  
on  t1.SeriesCD = t4.ITEM_CLASSIFICATION_CD

