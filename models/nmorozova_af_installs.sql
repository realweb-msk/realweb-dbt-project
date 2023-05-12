{{ config(materialized='table') }}

WITH
  af_installs AS (
    SELECT
      date,
      campaign_name,
      adset_name,
      platform,
      source,
      SUM(CASE
          WHEN appsflyer_id IS NOT NULL THEN 1
          ELSE 0
        END) AS installs
    FROM
        {{ ref('stg_af_installs') }}
    WHERE
      is_realweb = TRUE
      AND source != 'other'
      AND is_ret_campaign = FALSE
    GROUP BY date, campaign_name, adset_name, platform, source
),

sources as (
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
  WHERE is_realweb = TRUE
      AND is_ret_campaign = FALSE
  GROUP BY date, campaign_name, adset_name, platform
  
UNION ALL 

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
  WHERE is_realweb = TRUE
      AND is_ret_campaign = FALSE
  GROUP BY date, campaign_name, adset_name, platform

UNION ALL

    SELECT
      date,
      campaign_name,
      '-' AS adset_name,
      platform,
      'huawei_ads' AS source,
      SUM(clicks) AS clicks,
      SUM(costs) AS costs,
      NULL AS installs,
      SUM(impressions) AS impressions
    FROM
      {{ ref('stg_huawei_ads') }}
    WHERE
      is_realweb = TRUE
      AND is_ret_campaign = FALSE
    GROUP BY date, campaign_name, adset_name, platform

UNION ALL

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
    FROM
      {{ ref('stg_mytarget') }}
    WHERE
      is_realweb = TRUE
      AND is_ret_campaign = FALSE
    GROUP BY date, campaign_name, adset_name, platform

UNION ALL

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
    FROM
      {{ ref('stg_tiktok') }}
    WHERE
      is_realweb = TRUE
      AND is_ret_campaign = FALSE
    GROUP BY date, campaign_name, adset_name, platform

UNION ALL

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
    FROM
      {{ ref('stg_vkontakte') }}
    WHERE
      is_realweb = TRUE
      AND is_ret_campaign = FALSE
    GROUP BY date, campaign_name, adset_name, platform

UNION ALL 

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
    FROM
      {{ ref('stg_yandex') }}
    WHERE
      is_realweb = TRUE
      AND is_ret_campaign = FALSE
    GROUP BY date, campaign_name, adset_name, platform
),

af_installs_dashboard AS (
SELECT
af_installs.date,
af_installs.campaign_name,
af_installs.adset_name,
af_installs.platform,
af_installs.source,
COALESCE(sources.clicks, 0) AS clicks,
COALESCE(sources.impressions, 0) AS impressions,
COALESCE(sources.installs, af_installs.installs, 0) AS installs,
COALESCE(sources.costs, 0) AS costs
FROM sources
FULL JOIN af_installs
ON sources.date = af_installs.date
AND sources.campaign_name = af_installs.campaign_name
AND sources.adset_name = af_installs.adset_name
AND sources.platform = af_installs.platform
AND sources.source = af_installs.source
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
FROM af_installs_dashboard
WHERE platform IN ('ios','android')