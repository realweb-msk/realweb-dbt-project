/*
Целевая таблица: 
  клики, 
  показы, 
  установки, 
  расходы в разбивке 
    по дате, 
    кампании, 
    группе объявления, 
    платформе 
    источнику (рекламному кабинету). 

В таблице должны быть 
только кампании Риалвеба (is_realweb=TRUE), 
только User Acquisition (is_ret_campaign=FALSE),
только с известным источником (source!='other'). 

Если данные о количестве установок есть в таблицах из рекламных кабинетов, 
берите их оттуда, а если нет - то из stg_af_installs
*/

{% if target.name == 'prod' %}

{{
  config(
    materialized='table',
    partition_by = {
            "field": "date",
            "data_type": "date",
            "granularity": "day"
            }
  )
}}

{% endif %}

WITH

af AS ( -- 7327 rows
    SELECT 
        date,
        campaign_name,
        adset_name,
        platform,
        source,
        COUNT(DISTINCT appsflyer_id) as installs
    FROM {{ ref('stg_af_installs') }}
    WHERE is_realweb
        AND NOT is_ret_campaign
        AND source != 'other'
    GROUP BY 1, 2, 3, 4, 5
),

facebook AS ( -- 21045 rows, seems ok
    SELECT 
        date,
        campaign_name,
        adset_name,
        platform,
        'facebook' AS source,
        SUM(clicks) AS clicks,
        SUM(costs) AS costs,
        SUM(installs) AS installs,
        SUM(impressions) AS impressions
    FROM {{ ref('stg_facebook') }}
    WHERE is_realweb AND NOT is_ret_campaign
    GROUP BY 1, 2, 3, 4, 5
),

google_ads AS ( -- 3899 row, seems ok
    SELECT 
        date,
        campaign_name,
        adset_name,
        platform,
        'google_ads' AS source,
        SUM(clicks) AS clicks,
        SUM(costs) AS costs,
        SUM(installs) AS installs,
        SUM(impressions) AS impressions
    FROM {{ ref('stg_google_ads') }}
    WHERE is_realweb AND NOT is_ret_campaign
    GROUP BY 1, 2, 3, 4, 5
),

huawei AS ( -- 425, ok
    SELECT 
        date,
        campaign_name,
        '-' AS adset_name, -- единообразно с mytarget, vkontakte
        platform,
        'huawei' AS source,
        SUM(clicks) AS clicks,
        SUM(costs) AS costs,
        NULL AS installs,
        SUM(impressions) AS impressions
    FROM {{ ref('stg_huawei_ads') }}
    WHERE is_realweb AND NOT is_ret_campaign
    GROUP BY 1, 2, 3, 4, 5
),

mytarget AS ( -- 4773, по сути отсутсвует adset_name = '-' для всех событий
    SELECT 
        date,
        campaign_name,
        adset_name,
        platform,
        'mytarget' AS source,
        SUM(clicks) AS clicks,
        SUM(costs) AS costs,
        NULL AS installs,
        SUM(impressions) AS impressions
    FROM {{ ref('stg_mytarget') }}
    WHERE is_realweb AND NOT is_ret_campaign
    GROUP BY 1, 2, 3, 4, 5
),

tiktok AS ( -- 382 rows, ok
    SELECT 
        date,
        campaign_name,
        adset_name,
        platform,
        'tiktok' AS source,
        SUM(clicks) AS clicks,
        SUM(costs) AS costs,
        NULL AS installs,
        SUM(impressions) AS impressions
    FROM {{ ref('stg_tiktok') }}
    WHERE is_realweb AND NOT is_ret_campaign
    GROUP BY 1, 2, 3, 4, 5
),

vkontakte AS ( -- 94 , по сути отсутсвует adset_name = '-' для всех событий
    SELECT 
        date,
        campaign_name,
        adset_name,
        platform,
        'vkontakte' AS source,
        SUM(clicks) AS clicks,
        SUM(costs) AS costs,
        NULL AS installs,
        SUM(impressions) AS impressions
    FROM {{ ref('stg_vkontakte') }}
    WHERE is_realweb AND NOT is_ret_campaign
    GROUP BY 1, 2, 3, 4, 5
),

yandex AS ( -- 5002, есть события adset_name = '--' (453)
    SELECT 
        date,
        campaign_name,
        adset_name,
        platform,
        'yandex' AS source,
        SUM(clicks) AS clicks,
        SUM(costs) AS costs,
        NULL AS installs,
        SUM(impressions) AS impressions
    FROM {{ ref('stg_yandex') }}
    WHERE is_realweb AND NOT is_ret_campaign
    GROUP BY 1, 2, 3, 4, 5
),

all_sources AS ( -- 35620,  4 события без платформы, 5739 непустых
    SELECT * FROM facebook
    UNION ALL 
    SELECT * FROM google_ads
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

all_sources_with_af AS ( -- 40236 rows, 20854 пустых
    SELECT 
        date,
        campaign_name,
        adset_name,
        platform,
        source,
        COALESCE(clicks, 0) AS clicks,
        COALESCE(costs, 0) AS costs,
        COALESCE(all_sources.installs, af.installs, 0) AS installs, -- рекламный кабинет приоритетней AF для заказчика
        COALESCE(impressions, 0) AS impressions
    FROM all_sources
    FULL JOIN af
    USING(date, campaign_name, adset_name, platform, source)
)

SELECT -- 19383 rows
    date,
    campaign_name,
    adset_name,
    platform,
    source,
    clicks,
    costs,
    installs,
    impressions 
FROM all_sources_with_af
WHERE clicks + costs + installs + impressions > 0