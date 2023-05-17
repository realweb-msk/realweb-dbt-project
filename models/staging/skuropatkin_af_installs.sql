--dbt у меня так и не установился, поэтому вот просто код -.- 

{{ config(materialized='table', partition_by = {
            "field": "date",
            "data_type": "date",
            "granularity": "day"
            },) }}

{% if target.name == 'prod' %}

{% endif %}

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


-- собираем в одну таблицу

    all AS (
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

    
-- AppsFlyer количество установок 

    af_installs AS (
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

    result AS (
        SELECT
            date,
            campaign_name,
            COALESCE(COALESCE(all.adset_name, af_installs.adset_name), '-') AS adset_name,
            platform,
            source,
            COALESCE(clicks, 0) AS clicks,
            COALESCE(impressions, 0) AS impressions,
            COALESCE(COALESCE(all.installs, af_installs.cnt_installs), 0) AS installs,
            COALESCE(costs, 0.0) AS costs
        FROM all 
        LEFT JOIN af_installs 
        USING(date, campaign_name, adset_name, platform, source)	      
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
FROM result
