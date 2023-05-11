--избавляемся от ненужной информации в основной таблице stg_af_installs
WITH full_installs AS (
    SELECT *
    FROM {{ ref('stg_af_installs') }}
    WHERE is_realweb AND NOT is_ret_campaign AND NOT source = 'other'
),

--группируем таблицу по нужным полям и находим количество установок
--предполагаем, что один appsflyer_id - одна установка
installs_grouped AS (
    SELECT 
        date,
        campaign_name,
        adset_name,
        platform,
        source,
       COUNT(appsflyer_id) as installs
    FROM full_installs
    GROUP BY date, 
         campaign_name,
         adset_name,
         platform,
         source
),

--подготоваливаем таблицы по отдельным кабинетам
facebook AS (
    SELECT 
        date,
        campaign_name,
        adset_name,
        platform,
        'facebook' AS source,
        (clicks),
        (impressions),
        (installs),
        costs
    FROM {{ ref('stg_facebook') }}
    WHERE is_realweb AND NOT is_ret_campaign
), 

google AS (
    SELECT 
        date,
        campaign_name,
        adset_name,
        platform,
        'google_ads' AS source,
        clicks,
        impressions,
        installs,
        costs
    FROM {{ ref('stg_google_ads') }}
    WHERE is_realweb AND NOT is_ret_campaign
),

huawei AS (
    SELECT 
        date,
        campaign_name,
        'none' AS adset_name,
        platform,
        'huawei_ads' AS source,
        clicks,
        impressions,
        NULL AS installs,
        costs
    FROM {{ ref('stg_huawei_ads') }}
    WHERE is_realweb AND NOT is_ret_campaign
),

mytarget AS (
    SELECT 
        date,
        campaign_name,
        adset_name,
        platform,
        'mytarget' AS source,
        clicks,
        impressions,
        NULL AS installs,
        costs
    FROM {{ ref('stg_mytarget') }}
    WHERE is_realweb AND NOT is_ret_campaign
),

tiktok AS (
   SELECT 
        date,
        campaign_name,
        adset_name,
        platform,
        'tiktok' AS source,
        clicks,
        impressions,
        NULL AS installs,
        costs
    FROM {{ ref('stg_tiktok') }}
    WHERE is_realweb AND NOT is_ret_campaign 
),

vkontakte AS (
    SELECT 
        date,
        campaign_name,
        adset_name,
        platform,
        'vkontakte' AS source,
        clicks,
        impressions,
        NULL AS installs,
        costs
    FROM {{ ref('stg_vkontakte') }}
    WHERE is_realweb AND NOT is_ret_campaign 
),

yandex AS (
    SELECT 
        date,
        campaign_name,
        adset_name,
        platform,
        'yandex' AS source,
        clicks,
        impressions,
        NULL AS installs,
        costs
    FROM {{ ref('stg_yandex') }}
    WHERE is_realweb AND NOT is_ret_campaign 
),

--объединяем "вертикально" таблицы по отдельным кабинетам
all_sources AS (
    SELECT * FROM facebook
    UNION ALL 
    SELECT * FROM google
    UNION ALL 
    SELECT * FROM huawei
    UNION ALL 
    SELECT * FROM mytarget
    UNION ALL 
    SELECT * FROM tiktok
    UNION ALL 
    SELECT * FROM vkontakte
    UNION ALL
    SELECT * FROM yandex
),

--объединяем таблицы installs_grouped и all_sources по всем столбцам, по которым сгруппированы данные
--используем функцию coalesce, чтобы в данных оказалось первое ненулевое из имеющихся значений,
--а также заменяем пропуски на 0 этой же функцией
full_grouped AS (
    SELECT
        COALESCE(installs_grouped.date, all_sources.date) AS date,
        COALESCE(installs_grouped.campaign_name, all_sources.campaign_name) AS campaign_name,
        COALESCE(installs_grouped.adset_name, all_sources.adset_name) AS adset_name,
        COALESCE(installs_grouped.platform, all_sources.platform) AS platform,
        COALESCE(installs_grouped.source, all_sources.source) AS source,
        COALESCE(all_sources.clicks, 0) AS clicks,
        COALESCE(all_sources.impressions, 0) AS impressions,
        COALESCE(all_sources.installs, installs_grouped.installs, 0) AS installs,
        COALESCE(all_sources.costs, 0) AS costs
    FROM installs_grouped 
    FULL OUTER JOIN all_sources
    ON installs_grouped.date = all_sources.date
    AND installs_grouped.campaign_name = all_sources.campaign_name
    AND installs_grouped.adset_name = all_sources.adset_name
    AND installs_grouped.platform = all_sources.platform
    AND installs_grouped.source = all_sources.source
)

--из таблицы full_grouped получаем нужные данные, исключаем кампании, которые не дали никаких результатов
SELECT 
    date,
    campaign_name,
    adset_name,
    platform,
    source,
    clicks,
    impressions,
    installs,
    costs
FROM full_grouped
WHERE clicks + impressions + installs + costs > 0
ORDER BY date, campaign_name, adset_name, platform, source
