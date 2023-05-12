WITH sor AS
(SELECT 'facebook' AS source,
        date,
        campaign_name,
        adset_name,
        platform,
        clicks,
        impressions,
        costs     
FROM {{ ref('stg_facebook') }} 
WHERE is_realweb=TRUE
AND is_ret_campaign=FALSE
UNION ALL
SELECT 'google_abs' AS source,
        date,
        campaign_name,
        adset_name,
        platform,
        clicks,
        impressions,
        costs     
FROM {{ ref('stg_google_ads')}}
WHERE is_realweb=TRUE
AND is_ret_campaign=FALSE
UNION ALL
SELECT 'huawei_abs' AS source,
        date,
        campaign_name,
        '-' AS adset_name,
        platform,
        clicks,
        impressions,
        costs     
FROM {{ref("stg_huawei_ads")}}
WHERE is_realweb=TRUE
AND is_ret_campaign=FALSE
UNION ALL
SELECT 'mytarget' AS source,
        date,
        campaign_name,
        adset_name,
        platform,
        clicks,
        impressions,
        costs     
FROM {{ref("stg_mytarget")}}
WHERE is_realweb=TRUE
AND is_ret_campaign=FALSE
UNION ALL
SELECT 'tiktok' AS source,
        date,
        campaign_name,
        adset_name,
        platform,
        clicks,
        impressions,
        costs     
FROM {{ref("stg_tiktok")}}
WHERE is_realweb=TRUE
AND is_ret_campaign=FALSE
UNION ALL
SELECT 'vkontakte' AS source,
        date,
        campaign_name,
        adset_name,
        platform,
        clicks,
        impressions,
        costs     
FROM {{ref("stg_vkontakte")}}
WHERE is_realweb=TRUE
AND is_ret_campaign=FALSE
UNION ALL
SELECT 'yandex' AS source,
        date,
        campaign_name,
        adset_name,
        platform,
        clicks,
        impressions,
        costs     
FROM {{ref("stg_yandex")}}
WHERE is_realweb=TRUE
AND is_ret_campaign=FALSE),
ins AS 
(SELECT date,
       campaign_name,
       adset_name,
       platform,
       source,
       COUNT(*) AS installs      
FROM {{ref("stg_af_installs")}}
WHERE source!='other'
GROUP BY 1,2,3,4,5),
sor_2 AS
(SELECT 
        date,
        campaign_name,
        adset_name,
        platform,
        source,
        SUM(clicks) AS clicks,
        SUM(impressions) AS impressions,
        SUM(costs) AS costs
FROM sor 
GROUP BY 1,2,3,4,5)
SELECT  
        date,
        campaign_name,
        adset_name,
        platform,
        source,
        clicks,
        impressions,
        costs,
        installs 
FROM sor_2 LEFT JOIN ins USING(source,date, campaign_name, adset_name, platform)

