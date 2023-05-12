-- партицирование
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

-- агрегированные данные по fb, все из одной таблицы
WITH facebook AS (
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
    FROM {{ref('stg_facebook')}}
    WHERE is_realweb AND NOT is_ret_campaign
    GROUP BY date, campaign_name, adset_name, platform, source
), 

-- агрегированные данные по google, все из одной таблицы 
google AS (
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
    FROM {{ref('stg_google_ads')}}
    WHERE is_realweb AND NOT is_ret_campaign
    GROUP BY date, campaign_name, adset_name, platform, source
),

-- агрегированные данные по huawei, из таблицы stg_huawei_ads, не хватает adset_name и installs
huawei AS (
  SELECT 
      date,
      campaign_name,
      platform,
      'huawei_ads' AS source,
      SUM(clicks) AS clicks,
      SUM(costs) AS costs,
      SUM(impressions) AS impressions
  FROM {{ref('stg_huawei_ads')}}
  WHERE is_realweb AND NOT is_ret_campaign
  GROUP BY date, campaign_name, platform, source
),

-- данные по adset_name и installs для huawei из общей таблицы stg_af_installs
huawei_installs AS (
  SELECT 
      date,
      campaign_name,
      adset_name,
      platform,
      media_source AS source,
      COUNT(DISTINCT(appsflyer_id)) as installs,
  FROM {{ref('stg_af_installs')}} 
  WHERE is_realweb AND NOT is_ret_campaign AND (source = 'huawei_ads') AND (source!='other')
  GROUP BY date, campaign_name, platform, adset_name, source
),

-- джойн для получения итоговой таблицы по huawei
huawei_final AS (
  SELECT 
      date,
      campaign_name,
      adset_name,
      platform,
      source,
      SUM(clicks) AS clicks,
      SUM(costs) AS costs,
      SUM(installs) AS installs,
      SUM(impressions) AS impressions
  FROM huawei LEFT JOIN huawei_installs USING (date, campaign_name, platform, source)
  GROUP BY date, campaign_name, platform, adset_name, source    
),

-- объединенные и агрегированные данные по tiktok, vk, yandex из соответствующих таблиц, не хватает данных об installs
tiktok_VK_ya AS (
  SELECT 
      date,
      campaign_name,
      adset_name,
      platform,
      'TIKTOK' AS source,
      SUM(clicks) AS clicks,
      SUM(costs) AS costs,
      SUM(impressions) AS impressions
  FROM {{ref('stg_tiktok')}}
  WHERE is_realweb AND NOT is_ret_campaign
  GROUP BY date, campaign_name, adset_name, platform, source
  UNION ALL
  SELECT 
      date,
      campaign_name,
      adset_name,
      platform,
      'vkontakte' AS source,
      SUM(clicks) AS clicks,
      SUM(costs) AS costs,
      SUM(impressions) AS impressions
  FROM {{ref('stg_vkontakte')}}
  WHERE is_realweb AND NOT is_ret_campaign
  GROUP BY date, campaign_name, adset_name, platform, source
  UNION ALL
  SELECT 
      date,
      campaign_name,
      adset_name,
      platform,
      'yandex' AS source,
      SUM(clicks) AS clicks,
      SUM(costs) AS costs,
      SUM(impressions) AS impressions
  FROM {{ref('stg_yandex')}}
  WHERE is_realweb AND NOT is_ret_campaign
  GROUP BY date, campaign_name, adset_name, platform, source
),

-- данные по installs для tiktok, vk, yandex из общей таблицы stg_af_installs
tiktok_vk_ya_installs AS (
  SELECT 
      date,
      campaign_name,
      adset_name,
      platform,
      media_source AS source,
      COUNT(DISTINCT(appsflyer_id)) as installs
  FROM {{ref('stg_af_installs')}} 
  WHERE is_realweb AND NOT is_ret_campaign AND 
        ((source = 'TIKTOK') OR (source = 'vkontakte') OR (source = 'yandex')) AND (source !='other')
  GROUP BY date, campaign_name, adset_name, platform, source
), 

-- джойн для получения итоговой таблицы по tiktok, vk, yandex
tiktok_vk_ya_final AS (
  SELECT 
      date,
      campaign_name,
      adset_name,
      platform,
      source,
      SUM(clicks) AS clicks,
      SUM(costs) AS costs,
      SUM(installs) AS installs,
      SUM(impressions) AS impressions
  FROM tiktok_VK_ya LEFT JOIN tiktok_vk_ya_installs USING (date, campaign_name, adset_name, platform, source)
  GROUP BY date, campaign_name, platform, adset_name, source    
),

-- агрегированные данные по mytarget, из таблицы stg_mytarget_ads, не хватает installs
mytarget AS (
  SELECT 
      date,
      campaign_name,
      adset_name,
      platform,
      'mytarget' AS source,
      SUM(clicks) AS clicks,
      SUM(costs) AS costs,
      SUM(impressions) AS impressions
  FROM {{ref('stg_mytarget')}}
  WHERE is_realweb AND NOT is_ret_campaign
  GROUP BY date, campaign_name, adset_name, platform, source
), 

-- данные по installs для mytarget из общей таблицы stg_af_installs
mytarget_installs AS (
  SELECT 
      date,
      campaign_name,
      adset_name,
      platform,
      media_source AS source,
      COUNT(DISTINCT(appsflyer_id)) as installs
  FROM {{ref('stg_af_installs')}} 
  WHERE is_realweb AND NOT is_ret_campaign AND 
        (source = 'mytarget') AND (source !='other')
  GROUP BY date, campaign_name, adset_name, platform, source
), 

-- джойн для получения итоговой таблицы по mytarget
mytarget_final AS (
  SELECT 
      date,
      campaign_name,
      adset_name,
      platform,
      source,
      SUM(clicks) AS clicks,
      SUM(costs) AS costs,
      SUM(installs) AS installs,
      SUM(impressions) AS impressions
  FROM mytarget LEFT JOIN mytarget_installs USING (date, campaign_name, adset_name, platform, source)
  GROUP BY date, campaign_name, platform, adset_name, source    
),

-- объединение всех подитоговых таблиц
all_installs AS(
  SELECT * FROM facebook
  UNION ALL
  SELECT * FROM google
  UNION ALL
  SELECT * FROM huawei_final
  UNION ALL
  SELECT * FROM tiktok_vk_ya_final
  UNION ALL
  SELECT * FROM mytarget_final
),

-- финальные агрегированные данные
final AS (
  SELECT
    date,
    campaign_name,
    adset_name,
    platform,
    source,
    SUM(clicks) AS clicks,
    SUM(costs) AS costs,
    SUM(installs) AS installs,
    SUM(impressions) AS impressions
  FROM all_installs
  GROUP BY date, campaign_name, adset_name, platform, source
)

SELECT 
    date,
    campaign_name,
    adset_name,
    platform,
    source,
    clicks,
    costs,
    installs,
    impressions
FROM final


-- падает "тест на платформы"