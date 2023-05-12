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

WITH all_sources AS (
    SELECT date, campaign_name, adset_name, platform, source, count(*) AS installs 
    FROM {{ ref('stg_af_installs') }}
    WHERE source != 'other' AND is_realweb AND NOT is_ret_campaign
    GROUP BY 1,2,3,4,5
)

SELECT 
    date, 
    campaign_name,
    adset_name,
    platform,
    {{ install_source('campaign_name') }} source,
    clicks, 
    impressions, 
    installs, 
    costs
FROM {{ ref('stg_facebook') }}
WHERE is_realweb AND NOT is_ret_campaign

UNION ALL 

SELECT 
    date, 
    campaign_name,
    adset_name,
    platform,
    {{ install_source('campaign_name') }} AS source,
    clicks, 
    impressions, 
    installs, 
    costs
FROM {{ ref('stg_google_ads') }}
WHERE is_realweb AND NOT is_ret_campaign

UNION ALL 

SELECT 
    inst.date, 
    inst.campaign_name,
    'none' AS adset_name,
    inst.platform,
    {{ install_source('inst.campaign_name') }} AS source,
    inst.clicks, 
    inst.impressions,
    af.installs, 
    inst.costs
FROM {{ ref('stg_huawei_ads') }} as inst
LEFT OUTER JOIN (
    SELECT *
    FROM all_sources
    WHERE source='huawei'
) as af
ON inst.date = af.date AND inst.campaign_name = af.campaign_name AND inst.platform = af.platform
WHERE inst.is_realweb AND NOT inst.is_ret_campaign

UNION ALL

SELECT 
    inst.date, 
    inst.campaign_name,
    inst.adset_name,
    inst.platform,
    {{ install_source('inst.campaign_name') }} source,
    inst.clicks, 
    inst.impressions,
    af.installs, 
    inst.costs
FROM {{ ref('stg_mytarget') }} as inst
LEFT OUTER JOIN (
    SELECT *
    FROM all_sources
    WHERE source='mytarget'
) as af
ON inst.date = af.date AND inst.campaign_name = af.campaign_name AND inst.adset_name = af.adset_name AND inst.platform = af.platform
WHERE inst.is_realweb AND NOT inst.is_ret_campaign

UNION ALL

SELECT 
    inst.date, 
    inst.campaign_name,
    inst.adset_name,
    inst.platform,
    {{ install_source('inst.campaign_name') }} source,
    inst.clicks, 
    inst.impressions,
    af.installs, 
    inst.costs
FROM {{ ref('stg_vkontakte') }} as inst
LEFT OUTER JOIN (
    SELECT *
    FROM all_sources
    WHERE source='vkontakte'
) as af
ON inst.date = af.date AND inst.campaign_name = af.campaign_name AND inst.adset_name = af.adset_name AND inst.platform = af.platform
WHERE inst.is_realweb AND NOT inst.is_ret_campaign

UNION ALL

SELECT 
    inst.date, 
    inst.campaign_name,
    inst.adset_name,
    inst.platform,
    {{ install_source('inst.campaign_name') }} source,
    inst.clicks, 
    inst.impressions,
    af.installs, 
    inst.costs
FROM {{ ref('stg_yandex') }} as inst
LEFT OUTER JOIN (
    SELECT *
    FROM all_sources
    WHERE source='yandex'
) as af
ON inst.date = af.date AND inst.campaign_name = af.campaign_name AND inst.adset_name = af.adset_name AND inst.platform = af.platform
WHERE inst.is_realweb AND NOT inst.is_ret_campaign