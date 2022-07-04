-- Партицирование данных по дате

{% if target.name == 'prod' %}

{{
  config( 
    materialized='table',
    partition_by = {
            "field": "date",
            "data_type": "date",
            "granularity": "day"
            },
  )
}}

{% endif %}


-- Установки приложений из АФ

WITH 
    installs AS (
        SELECT
            date,
            campaign_name,
            adset_name,
            platform,
            appsflyer_id,
            source
        FROM {{ ref('stg_af_installs') }}
        WHERE is_realweb
          AND NOT is_ret_campaign
          AND source != 'other'
    ),

-- Срезы данных

    slices AS (
        SELECT
            date,
            campaign_name,
            adset_name,
            platform,
            source,
            COUNT(DISTINCT appsflyer_id) count_installs
        FROM installs
        GROUP BY 1, 2, 3, 4, 5
    ),

-- Данные из кабинета Facebook

    facebook AS (
         SELECT
             date,
             campaign_name,
             adset_name,
             platform,
             'facebook' AS source,
             clicks,
             costs,
             installs,
             impressions
         FROM {{ ref('stg_facebook') }}
         WHERE is_realweb AND NOT is_ret_campaign
     ),

-- Данные из кабинета Google Ads

    google_ads AS (
        SELECT
            date,
            campaign_name,
            adset_name,
            platform,
            'google_ads' AS source,
            clicks,
            costs,
            installs,
            impressions
        FROM {{ ref('stg_google_ads') }}
        WHERE is_realweb AND NOT is_ret_campaign
    ),

-- Данные из кабинета Huawei Ads

    huawei AS (
         SELECT
             date,
             campaign_name,
             '' AS adset_name,
             platform,
             'huawei' AS source,
             clicks,
             costs,
             NULL AS installs,
             impressions
         FROM {{ ref('stg_huawei_ads') }}
         WHERE is_realweb AND NOT is_ret_campaign
     ),

-- Данные из кабинета MyTarget

    mytarget AS (
         SELECT
             date,
             campaign_name,
             adset_name,
             platform,
             'mytarget' AS source,
             clicks,
             costs,
             NULL AS installs,
             impressions
         FROM {{ ref('stg_mytarget') }}
         WHERE is_realweb AND NOT is_ret_campaign
     ),

-- Данные из кабинета TikTok

    tiktok AS (
         SELECT
             date,
             campaign_name,
             adset_name,
             platform,
             'tiktok' AS source,
             clicks,
             costs,
             NULL AS installs,
             impressions
         FROM {{ ref('stg_tiktok') }}
         WHERE is_realweb AND NOT is_ret_campaign
     ),

-- Данные из кабинета VK 

    vkontakte AS (
         SELECT
             date,
             campaign_name,
             adset_name,
             platform,
             'vkontakte' AS source,
             clicks,
             costs,
             NULL AS installs,
             impressions
         FROM {{ ref('stg_vkontakte') }}
         WHERE is_realweb AND NOT is_ret_campaign
     ),

-- Данные из кабинета Яндекса 

    yandex AS (
         SELECT
             date,
             campaign_name,
             adset_name,
             platform,
             'yandex' AS source,
             clicks,
             costs,
             NULL AS installs,
             impressions
         FROM {{ ref('stg_yandex') }}
         WHERE is_realweb AND NOT is_ret_campaign
     ),

-- Объединение всех источников

    joined_sources AS (
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

-- Соединяем данные и срезы
    joined_data AS (
         SELECT
             CASE
                 WHEN slices.date IS NOT NULL THEN slices.date
                 ELSE joined_sources.date
                 END AS date,
             CASE
                 WHEN slices.campaign_name IS NOT NULL THEN slices.campaign_name
                 ELSE joined_sources.campaign_name
                 END AS campaign_name,
             CASE
                 WHEN slices.adset_name IS NOT NULL THEN slices.adset_name
                 ELSE joined_sources.adset_name
                 END AS adset_name,
             CASE
                 WHEN slices.platform IS NOT NULL THEN slices.platform
                 ELSE joined_sources.platform
                 END AS platform,
             CASE
                 WHEN slices.source IS NOT NULL THEN slices.source
                 ELSE joined_sources.source
                 END AS source,
             CASE
                 WHEN clicks IS NOT NULL THEN clicks
                 ELSE 0
                 END AS clicks,
             CASE
                 WHEN costs IS NOT NULL THEN costs
                 ELSE 0
                 END AS costs,
             CASE
                 WHEN installs IS NOT NULL THEN installs
                 WHEN count_installs IS NOT NULL THEN count_installs
                 ELSE 0
                 END AS installs,
             CASE
                 WHEN impressions IS NOT NULL THEN impressions
                 ELSE 0
                 END AS impressions
         FROM slices
         FULL JOIN joined_sources
                ON slices.date = joined_sources.date
                AND slices.adset_name = joined_sources.adset_name
                AND slices.platform = joined_sources.platform
                AND slices.source = joined_sources.source
                AND slices.campaign_name = joined_sources.campaign_name
     )

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
FROM joined_data
