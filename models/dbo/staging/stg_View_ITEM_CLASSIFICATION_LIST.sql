
-- SQL Server   BusinessManagement  View_ITEM_CLASSIFICATION_LIST 


select distinct
    left(T0.ITEM_CLASSIFICATION_CD, 7) as ITEM_CLASSIFICATION_CD, 
    '2' as ZZKAISO, 
    case left(T0.ITEM_CLASSIFICATION_CD, 7) 
        when '1049100' then 'FTS' 
        when '1049103' then 'MFTS CAMERA' 
        when '1049104' then 'MFTS LENS' 
        when '1049105' then 'MFTS ACCESSORIES' 
    else T1.BUSINESS_SEG_TXT end as DESCRIPTION
from 
    {{ source('dbo', 'DBO_NUM_ITEM_CLASSIFICATION') }} as T0
    -- [dbo].[NUM_ITEM_CLASSIFICATION] as T0 
left join
    -- [dbo].[BUSINESS_SEGMENT] as T1 
    {{ source('dbo', 'DBO_BUSINESS_SEGMENT') }} as T1
on 
    T1.BUSINESS_SEG_CD = left(T0.ITEM_CLASSIFICATION_CD, 4)
where             
    SUBSTRING(T0.ITEM_CLASSIFICATION_CD, 5, 2) = '10' 
    AND T0.ZZKAISO = '3' 
    AND T0.DELETE_FLAG = 0 
    AND T1.IMG_FLG = '1'

union all
select
    RTRIM(T0.ITEM_CLASSIFICATION_CD) as ITEM_CLASSIFICATION_CD, 
    T0.ZZKAISO, 
    T0.DESCRIPTION
from
     {{ source('dbo', 'DBO_NUM_ITEM_CLASSIFICATION') }} as T0
    -- [dbo].[NUM_ITEM_CLASSIFICATION] as T0 
left join
    -- [dbo].[BUSINESS_SEGMENT] as T1 
     {{ source('dbo', 'DBO_BUSINESS_SEGMENT') }} as T1
ON 
    T1.BUSINESS_SEG_CD = LEFT(T0.ITEM_CLASSIFICATION_CD, 4)
where             
    SUBSTRING(T0.ITEM_CLASSIFICATION_CD, 5, 2) = '10' 
    AND T0.DELETE_FLAG = 0 
    AND T1.IMG_FLG = '1'


union all
select distinct
    RTRIM(T0.ITEM_CLASSIFICATION_CD) || ifnull(T0.COLOR_NAME_EN, '') as ITEM_CLASSIFICATION_CD, 
    '6' as ZZKAISO, 
    T1.DESCRIPTION || ' ' || ifnull(T0.COLOR_NAME_EN, '') as DESCRIPTION
from
     {{ source('dbo', 'DBO_ITEM_APPLICATION') }} as T0
            --   [dbo].[ITEM_APPLICATION] as T0 
left join
     {{ source('dbo', 'DBO_NUM_ITEM_CLASSIFICATION') }} as T1
    -- [dbo].[NUM_ITEM_CLASSIFICATION] as T1 
on
    T1.ITEM_CLASSIFICATION_CD = T0.ITEM_CLASSIFICATION_CD
where
    T0.ITEM_CLASSIFICATION_CD <> '' 
    AND T0.COLOR_NAME_EN <> ''


