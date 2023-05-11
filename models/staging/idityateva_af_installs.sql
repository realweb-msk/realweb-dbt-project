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

/*во временные таблицы агрегируем данные по каждому рекламному кабинету с разбивкой по дате, названию кампании
группе объявлений и платформе. Добавим поле с названием рекламного кабинета (source). Где необходимо
 добавим поля с отсутсвующими значениями installs и adset_name*/
WITH
    facebook AS (
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
        WHERE is_realweb
        AND NOT is_ret_campaign
        GROUP BY 1,2,3,4
     ),

    google_ads AS (
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
        WHERE is_realweb
        AND NOT is_ret_campaign
	GROUP BY 1,2,3,4
    ),

    huawei AS (
        SELECT
            date,
            campaign_name,
            '-' AS adset_name,
            platform,
            'huawei' AS source,
            SUM(clicks) AS clicks,
            SUM(costs) AS costs,
            NULL AS installs,
            SUM(impressions) AS impressions
        FROM {{ ref('stg_huawei_ads') }}
        WHERE is_realweb
        AND NOT is_ret_campaign
	GROUP BY 1,2,3,4
     ),

    mytarget AS (
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
        WHERE is_realweb
        AND NOT is_ret_campaign
	GROUP BY 1,2,3,4
     ),

    tiktok AS (
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
        WHERE is_realweb
        AND NOT is_ret_campaign
	GROUP BY 1,2,3,4
     ),

    vkontakte AS (
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
	GROUP BY 1,2,3,4
     ),

    yandex AS (
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
	GROUP BY 1,2,3,4
     ),

/*склеиваем данные по всем рекламным кабинетам в одну таблицу */
    all_source AS (
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
/*Считаем количество установок из appsflyer */
    t_installs AS (
        SELECT 
            date,
            campaign_name,
            adset_name,
            platform,
	    source,
            COUNT(DISTINCT appsflyer_id) AS cnt_installs       
	    FROM {{ ref('stg_af_installs') }} 
	    WHERE is_realweb
        AND NOT is_ret_campaign
        AND source != 'other'
	    GROUP BY 1,2,3,4,5 
      ),
/*Отсутсвующие значения по данным установок и названию групп рекламных объявлений 
из рекламных кабинетов заменяем на данные по установкам из AppsFlyer.
Отсутствующие значения по кликам, показам и стоимости рекламы заменяем на 0*/
    final_table AS (
        SELECT
            date,
            campaign_name,
            COALESCE(COALESCE(a.adset_name, i.adset_name), '-') AS adset_name,
            platform,
            source,
            COALESCE(clicks, 0) AS clicks,
            COALESCE(impressions, 0) AS impressions,
            COALESCE(COALESCE(a.installs, i.cnt_installs), 0) AS installs,
            COALESCE(costs, 0.0) AS costs
        FROM all_source AS a
        LEFT JOIN t_installs AS i
        USING(date, campaign_name, adset_name, platform, source)	      
    )
/*Итоговая таблица*/
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
FROM final_table