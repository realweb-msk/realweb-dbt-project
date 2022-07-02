-- Материализация в Production датасете с партицированием по датам
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

--клики, показы, установки, расходы в разбивке по дате, кампании, группе объявления, платформе и источнику (рекламному кабинету) 
--в таблице должны быть только кампании Риалвеба (is_realweb=TRUE), только с известным источником (source!='other'), только User Acquisition (is_ret_campaign=FALSE)
--соберем данные с задаными условиями по каждому источнику
WITH 
  facebook AS(
    SELECT 
      date,
      campaign_name, 
      adset_name, 
      platform,
      'facebook' AS source,
      clicks, 
      impressions, 
      installs, 
      costs
    FROM {{ ref('stg_facebook') }} 
    WHERE is_realweb = TRUE AND is_ret_campaign = FALSE),
  google AS(
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
    WHERE is_realweb = TRUE AND is_ret_campaign = FALSE),
  huawei AS(
    SELECT 
      date,
      campaign_name, 
      '' AS adset_name, 
      platform,
      'huawei' AS source,
      clicks, 
      impressions, 
      NULL AS installs, 
      costs
    FROM {{ ref('stg_huawei_ads') }}
    WHERE is_realweb = TRUE AND is_ret_campaign = FALSE),
  mytarget AS(
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
    WHERE is_realweb = TRUE AND is_ret_campaign = FALSE),
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
    WHERE is_realweb = TRUE AND is_ret_campaign = FALSE),
  vk AS (
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
    WHERE is_realweb = TRUE AND is_ret_campaign = FALSE),
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
    WHERE is_realweb = TRUE AND is_ret_campaign = FALSE),
-- соеденим источники
  all_source AS (
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
    SELECT * FROM yandex),
-- считаем количество установок (как количество уникальный id)
  count_inst_ap AS (
    SELECT 
      date,
      campaign_name, 
      adset_name,
      platform,
      source,
      COUNT(DISTINCT appsflyer_id) AS cnt_installs  
    FROM {{ ref('stg_af_installs') }} 
    WHERE is_realweb = TRUE AND is_ret_campaign = FALSE AND is_ret_campaign=FALSE AND source != 'other'
    GROUP BY 1, 2, 3, 4, 5),
-- собираем итоговую таблицу по условиям
  join_data AS (
    SELECT 
      CASE
        WHEN a.date IS NOT NULL
        THEN a.date
        ELSE cnt.date
      END AS date,
      CASE
        WHEN a.campaign_name IS NOT NULL 
        THEN a.campaign_name
        ELSE cnt.campaign_name
      END AS campaign_name,
      CASE
        WHEN a.adset_name IS NOT NULL
        THEN a.adset_name
        ELSE cnt.adset_name
      END AS adset_name,
      CASE
        WHEN a.platform IS NOT NULL
        THEN a.platform
        ELSE cnt.platform
      END AS platform,
      CASE
        WHEN a.source IS NOT NULL
        THEN a.source
        ELSE cnt.source
      END AS source,
      CASE
        WHEN clicks IS NOT NULL
        THEN clicks
        ELSE 0
      END AS clicks,
      CASE
        WHEN impressions IS NOT NULL
        THEN impressions
        ELSE 0 
      END AS impressions,
      CASE 
        WHEN installs IS NOT NULL
        THEN installs
        WHEN cnt_installs IS NOT NULL
        THEN cnt_installs
        ELSE 0
      END AS installs,
      CASE 
        WHEN costs IS NOT NULL
        THEN costs
        ELSE 0
      END AS costs

    FROM all_source a
    FULL JOIN count_inst_ap cnt
    ON a.date = cnt.date 
    AND a.campaign_name = cnt.campaign_name 
    AND a.adset_name = cnt.adset_name 
    AND a.platform = cnt.platform 
    AND a.source = cnt.source),
-- фильтрация по компаниям и платформам
  final AS (
  SELECT *
  FROM join_data
  WHERE campaign_name IS NOT NULL AND (platform = 'ios' OR platform = 'android'))
-- итоговая таблица, стоит отметить, что 19570 строк имеют нулевые значения по всем параметрам воронки, но в ТЗ не сказано их убирать
SELECT
  date,
  campaign_name, 
  adset_name, 
  platform,
  source, 
  impressions,
  clicks, 
  installs, 
  costs
FROM final