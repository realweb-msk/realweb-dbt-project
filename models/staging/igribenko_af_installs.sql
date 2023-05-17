{{ config   (
    materialized='table',
    partition_by=   {
                "field": "date",
                "data_type": "date",
                "granularity": "day"
                    }
            )
}}

-- считаем количество установок из appsflyer
WITH af_installs AS (
  SELECT date,
          source,
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

-- обьединяем данные из huawei, yandex и mytarget
-- агрегируем данные по площадкам не включая в агрегацию adset_name.
-- в stg_huawei_ads отсутствует информация о adset_name
-- в stg_af_installs отсутствует информация о adset_name по кабинету yandex
-- в stg_mytarget все значения adset_name равны `-`
-- исключим из вывода кампании, в которых не было показов/кликов (impressions != 0 OR clicks != 0)
huawei_yandex_mytarget_vkontakte AS (
  SELECT date,
          'huawei' AS source,
          campaign_name,
          CAST(NULL AS string) AS adset_name,
          platform,
          clicks,
          costs,
          impressions
  FROM {{ ref('stg_huawei_ads') }}
  WHERE is_ret_campaign IS FALSE
    AND is_realweb IS TRUE
    AND (impressions != 0 OR clicks != 0)
  UNION DISTINCT
  SELECT  date,
          'yandex' AS source,
          campaign_name,
          CAST(NULL AS string) AS adset_name,
          platform,
          SUM(clicks) AS clicks,
          SUM(costs) AS costs,
          SUM(impressions) AS impressions
  FROM {{ ref('stg_yandex') }}
  WHERE is_ret_campaign IS FALSE
    AND is_realweb IS TRUE
    AND (impressions != 0 OR clicks != 0)
  GROUP BY 1, 2, 3, 5
  UNION DISTINCT
  SELECT  date,
          'mytarget' AS source,
          campaign_name,
          CAST(NULL AS string) AS adset_name,
          platform,
          SUM(clicks) AS clicks,
          SUM(costs) AS costs,
          SUM(impressions) AS impressions 
  FROM {{ ref('stg_mytarget') }}
  WHERE is_ret_campaign IS FALSE
    AND is_realweb IS TRUE
    AND (impressions != 0 OR clicks != 0)
  GROUP BY 1, 2, 3, 5
  UNION DISTINCT
  SELECT  date,
          'vkontakte' AS source,
          campaign_name,
          CAST(NULL AS string) AS adset_name,
          platform,
          clicks,
          costs,
          impressions
  FROM {{ ref('stg_vkontakte') }}
  WHERE is_ret_campaign IS FALSE
    AND is_realweb IS TRUE
    AND (impressions != 0 OR clicks != 0)
  GROUP BY 1, 2, 3, 5
),

-- добавляем информацию о установках из stg_af_installs 
-- к данным о yandex, huawei и mytarget
huawei_yandex_mytarget_vkontakte_with_installs AS (
  SELECT date,
        source,
        campaign_name,
        adset_name,
        platform,
        clicks,
        costs,
        impressions,
        installs
  FROM huawei_yandex_mytarget_vkontakte
  FULL JOIN (SELECT date,
                    source,
                    campaign_name,
                    platform,
                    SUM(installs) AS installs 
              FROM af_installs
              WHERE source IN ('huawei', 'yandex', 'mytarget')
              GROUP BY 1, 2, 3, 4
            ) AS installs_af USING(date, campaign_name, platform, source)
),

-- добавим название кабинета для stg_tiktok
-- а также отфильтруем нужные значения
tiktok AS (
  SELECT  date,
          'tiktok' AS source,
          campaign_name,
          adset_name,
          platform,
          clicks,
          costs,
          impressions
  FROM {{ ref('stg_tiktok') }}
  WHERE is_ret_campaign IS FALSE
    AND is_realweb IS TRUE
    AND (impressions != 0 OR clicks != 0)
)

-- обьединяем данные по всем кабинетам в итоговую таблицу
-- к кабинету tiktok добавим данные о установках из appsflyer
SELECT date,
        source,
        campaign_name,
        adset_name,
        platform,
        clicks,
        costs,
        impressions,
        installs
FROM huawei_yandex_mytarget_vkontakte_with_installs
UNION DISTINCT
SELECT  date,
        source,
        campaign_name,
        adset_name,
        platform,
        clicks,
        costs,
        impressions,
        installs
FROM tiktok
FULL JOIN af_installs USING(date, campaign_name, adset_name, platform, source)
UNION DISTINCT
SELECT
    date,
    'facebook' AS source,
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
  AND (impressions != 0 OR clicks != 0)
UNION DISTINCT
SELECT
    date,
    'google_ads' AS source,
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
  AND (impressions != 0 OR clicks != 0)