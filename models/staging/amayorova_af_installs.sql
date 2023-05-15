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

WITH cnt_installs AS (
    SELECT
        date,
        campaign_name,
        adset_name,
        platform,
        source,
        COUNT(DISTINCT appsflyer_id) as installs
FROM {{ ref('stg_af_installs') }}
WHERE is_realweb AND NOT is_ret_campaign AND source != 'other'
GROUP BY date, campaign_name, adset_name, platform, source
),

facebook AS (
    SELECT
        date,
        campaign_name,
        adset_name,
        platform,
        'facebook' as source,
        clicks,
        costs,
        installs,
        impressions
FROM {{ ref('stg_facebook') }}
WHERE is_realweb AND NOT is_ret_campaign
),

google AS (
    SELECT
        date,
        campaign_name,
        adset_name,
        platform,
        'google' as source,
        clicks,
        costs,
        installs,
        impressions
FROM {{ ref('stg_google_ads') }}
WHERE is_realweb AND NOT is_ret_campaign
),

huawei AS (
    SELECT
        date,
        campaign_name,
        '' as adset_name,
        platform,
        'huawei' as source,
        clicks,
        costs,
        NULL as installs,
        impressions
FROM {{ ref('stg_huawei_ads') }}
WHERE is_realweb AND NOT is_ret_campaign
),

mytarget AS (
    SELECT
        date,
        campaign_name,
        adset_name,
        platform,
        'mytarget' as source,
        clicks,
        costs,
        NULL as installs,
        impressions
FROM {{ ref('stg_mytarget') }}
WHERE is_realweb AND NOT is_ret_campaign
),

tiktok AS (
    SELECT
        date,
        campaign_name,
        adset_name,
        platform,
        'tiktok' as source,
        clicks,
        costs,
        NULL as installs,
        impressions
FROM {{ ref('stg_tiktok') }}
WHERE is_realweb AND NOT is_ret_campaign
),

vk AS (
    SELECT
        date,
        campaign_name,
        adset_name,
        platform,
        'vkontakte' as source,
        clicks,
        costs,
        NULL as installs,
        impressions
FROM {{ ref('stg_vkontakte') }}
WHERE is_realweb AND NOT is_ret_campaign
),

yandex AS (
    SELECT
        date,
        campaign_name,
        adset_name,
        platform,
        'yandex' as source,
        clicks,
        costs,
        NULL as installs,
        impressions
FROM {{ ref('stg_yandex') }}
WHERE is_realweb AND NOT is_ret_campaign
),

merged_sources AS (
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
    SELECT * FROM vk
    UNION ALL 
    SELECT * FROM yandex
),

full_data AS (
    SELECT       
        mrg.date,
        mrg.campaign_name as campaign_name,
        mrg.adset_name,
        mrg.platform,
        mrg.source,
        COALESCE(mrg.installs, cnt.installs) as installs,
        impressions,
        clicks,
        costs
    FROM merged_sources as mrg
    FULL JOIN cnt_installs as cnt
    USING (date, campaign_name, adset_name, platform, source)
)

SELECT 
    date,
    campaign_name,
    adset_name,
    platform,
    source,
    installs,
    impressions,
    clicks,
    costs
FROM full_data
