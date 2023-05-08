/*Партицирование по дате*/
{{ config(
    materialized='partition_msolokhina_af_installs',
    partition_by={
      "field": "DATE",
      "data_type": "date",
      "granularity": "month"
    }
)}}

/* Объединяю все таблицы по источникам, где нет данных установок:*/
WITH Total_table AS(

SELECT 
DATE,
CAMPAIGN_NAME,
PLATFORM,
CLICKS,
IMPRESSIONS,
COSTS,
ADSET_NAME,
"yandex" AS SOURCE
FROM {{ ref('stg_yandex') }}
WHERE IS_REALWEB=True AND IS_RET_CAMPAIGN=False

UNION ALL

SELECT
DATE,
CAMPAIGN_NAME,
PLATFORM,
CLICKS,
IMPRESSIONS,
COSTS,
ADSET_NAME,
"vkontakte" AS SOURCE
FROM {{ ref('stg_vkontakte') }}
WHERE IS_REALWEB=True AND IS_RET_CAMPAIGN=False

UNION ALL

SELECT
DATE,
CAMPAIGN_NAME,
PLATFORM,
CLICKS,
IMPRESSIONS,
COSTS,
ADSET_NAME,
"tiktok" AS SOURCE
FROM {{ ref('stg_tiktok') }}
WHERE IS_REALWEB=True AND IS_RET_CAMPAIGN=False

UNION ALL

SELECT
DATE,
CAMPAIGN_NAME,
PLATFORM,
CLICKS,
IMPRESSIONS,
COSTS,
ADSET_NAME,
"mytarget" AS SOURCE
FROM {{ ref('stg_mytarget') }}
WHERE IS_REALWEB=True AND IS_RET_CAMPAIGN=False

UNION ALL

SELECT
DATE,
CAMPAIGN_NAME,
PLATFORM,
CLICKS,
IMPRESSIONS,
COSTS,
NULL as ADSET_NAME,
"huawei_ads" AS SOURCE

FROM {{ ref('stg_huawei_ads') }}
WHERE IS_REALWEB=True AND IS_RET_CAMPAIGN=False

)

/* Чтобы добавить кол-во установок делаю лефт джойн с моделью stg_af_installs */
SELECT 
A.DATE AS DATE,
A.CAMPAIGN_NAME AS CAMPAIGN_NAME,
A.PLATFORM AS PLATFORM,
CLICKS,
IMPRESSIONS,
COSTS,
A.ADSET_NAME AS ADSET_NAME,
SOURCE,
INSTALLS
FROM Total_table as A
LEFT JOIN 
( 
SELECT
DATE,
CAMPAIGN_NAME,
ADSET_NAME,
PLATFORM,
COUNT(APPSFLYER_ID) AS INSTALLS
FROM {{ ref('stg_af_installs') }} 
WHERE IS_REALWEB=True AND IS_RET_CAMPAIGN=False
GROUP BY DATE, CAMPAIGN_NAME, ADSET_NAME, PLATFORM
) AS B
ON (A.DATE=B.DATE AND A.CAMPAIGN_NAME=B.CAMPAIGN_NAME AND A.PLATFORM=B.PLATFORM AND A.ADSET_NAME=B.ADSET_NAME)

/*В таблице stg_af_installs я проверила есть только источники, которые у нас разбиты по отдельным таблицам и 
others, которые нам не нужны, поэтому больше из нее ничего не берем*/

/*Далее добавляю источники, в которых было прописано кол-во установок:*/
UNION ALL

SELECT
DATE,
CAMPAIGN_NAME,
PLATFORM,
CLICKS,
IMPRESSIONS,
COSTS,
ADSET_NAME,
"google_ads" AS SOURCE,
INSTALLS
FROM {{ ref('stg_google_ads') }}
WHERE IS_REALWEB=True AND IS_RET_CAMPAIGN=False

UNION ALL

SELECT
DATE,
CAMPAIGN_NAME,
PLATFORM,
CLICKS,
IMPRESSIONS,
COSTS,
ADSET_NAME,
"facebook" AS SOURCE,
INSTALLS
FROM {{ ref('stg_facebook') }}
WHERE IS_REALWEB=True AND IS_RET_CAMPAIGN=False



