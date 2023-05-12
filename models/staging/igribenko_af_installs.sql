{{ config   (
    materialized='table',
    partition_by=   {
                "field": "date",
                "data_type": "date",
                "granularity": "day"
                    }
            )
}}

WITH af_installs AS (
    SELECT date,
           media_source,
           campaign_name,
           adset_name,
           platform,
           COUNT(appsflyer_id) AS installs 
    FROM {{ ref('stg_af_installs') }}
    WHERE source != 'other'
      AND is_ret_campaign IS FALSE
      AND is_realweb IS TRUE
    GROUP BY 1, 2, 3, 4, 5
),

huawei AS (
    SELECT date,
           media_source,
           campaign_name,
           NULL AS adset_name,
           platform,
           clicks,
           costs,
           installs,
           impressions
    FROM {{ ref('stg_huawei_ads') }} AS hua
    LEFT JOIN (SELECT date,
                      media_source,
                      campaign_name,
                      platform,
                      SUM(installs) AS installs 
               FROM af_installs
               WHERE media_source = 'huawei'
               GROUP BY 1, 2, 3, 4
              ) AS inst USING(date, campaign_name, platform)
    WHERE is_ret_campaign IS FALSE
      AND is_realweb IS TRUE 
),

targ_tic_vk_yan AS (
SELECT  date,
        'mytarget' AS media_source,
        campaign_name,
        adset_name,
        platform,
        clicks,
        costs,
        impressions 
FROM {{ ref('stg_mytarget') }} AS mt
WHERE is_ret_campaign IS FALSE
  AND is_realweb IS TRUE
UNION DISTINCT
SELECT  date,
        'tictok' AS media_source,
        campaign_name,
        adset_name,
        platform,
        clicks,
        costs,
        impressions 
FROM {{ ref('stg_tiktok') }} AS tik 
WHERE is_ret_campaign IS FALSE
  AND is_realweb IS TRUE
UNION DISTINCT
SELECT  date,
        'vkontakte' AS media_source,
        campaign_name,
        adset_name,
        platform,
        clicks,
        costs,
        impressions 
FROM {{ ref('stg_vkontakte') }} AS vk
WHERE is_ret_campaign IS FALSE
  AND is_realweb IS TRUE
UNION DISTINCT
SELECT  date,
        'yandex' AS media_source,
        campaign_name,
        adset_name,
        platform,
        clicks,
        costs,
        impressions 
FROM {{ ref('stg_yandex') }} AS ya
WHERE is_ret_campaign IS FALSE
  AND is_realweb IS TRUE
)

SELECT  date,
        media_source,
        campaign_name,
        adset_name,
        platform,
        clicks,
        costs,
        installs,
        impressions 
FROM targ_tic_vk_yan
LEFT JOIN af_installs AS inst USING(date, campaign_name, adset_name, platform, media_source)
UNION DISTINCT
SELECT date,
    media_source,
    campaign_name,
    NULL AS adset_name,
    platform,
    clicks,
    costs,
    installs,
    impressions
FROM huawei
UNION DISTINCT
SELECT
    date,
    'facebook' AS media_source,
    campaign_name,
    adset_name,
    platform,
    clicks,
    costs,
    installs,
    impressions
FROM {{ ref('stg_facebook') }}
WHERE is_ret_campaign IS FALSE
  AND is_realweb IS TRUE 
UNION DISTINCT
SELECT
    date,
    'google_ads' AS media_source,
    campaign_name,
    adset_name,
    platform,
    clicks,
    costs,
    installs,
    impressions
FROM {{ ref('stg_google_ads') }}
WHERE is_ret_campaign IS FALSE
  AND is_realweb IS TRUE