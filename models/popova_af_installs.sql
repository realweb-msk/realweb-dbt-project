WITH 
facebook AS (
    SELECT 
        date, 
        campaign_name,
        adset_name,
        platform,
        'facebook' AS source,
        SUM(clicks) AS clicks,
        SUM(impressions) AS impressions,
        SUM(installs) AS installs,
        SUM(costs) AS costs
    FROM {{ ref('stg_facebook') }}
    WHERE is_realweb AND NOT is_ret_campaign
    GROUP BY 1, 2, 3, 4
),
google AS (
    SELECT 
        date, 
        campaign_name,
        adset_name,
        platform,
        'google' AS source,
        SUM(clicks) AS clicks,
        SUM(impressions) AS impressions,
        SUM(installs) AS installs,
        SUM(costs) AS costs
    FROM {{ ref('stg_google_ads') }}
    WHERE is_realweb AND NOT is_ret_campaign
    GROUP BY 1, 2, 3, 4
),
huawei AS (
    SELECT 
        date, 
        campaign_name,
        'NAN' AS adset_name,
        platform,
        'huawei' AS source,
        SUM(clicks) AS clicks,
        SUM(impressions) AS impressions,
        NULL AS installs,
        SUM(costs) AS costs
    FROM {{ ref('stg_huawei_ads') }}
    WHERE is_realweb AND NOT is_ret_campaign
    GROUP BY 1, 2, 3, 4
),
mytarget AS (
    SELECT 
        date, 
        campaign_name,
        adset_name,
        platform,
        'mytarget' AS source,
        SUM(clicks) AS clicks,
        SUM(impressions) AS impressions,
        NULL AS installs,
        SUM(costs) AS costs
    FROM {{ ref('stg_mytarget') }}
    WHERE is_realweb AND NOT is_ret_campaign
    GROUP BY 1, 2, 3, 4
),
tiktok AS (
    SELECT 
        date, 
        campaign_name,
        adset_name,
        platform,
        'tiktok' AS source,
        SUM(clicks) AS clicks,
        SUM(impressions) AS impressions,
        NULL AS installs,
        SUM(costs) AS costs
    FROM {{ ref('stg_tiktok') }}
    WHERE is_realweb AND NOT is_ret_campaign
    GROUP BY 1, 2, 3, 4
),
vk AS (
    SELECT 
        date, 
        campaign_name,
        adset_name,
        platform,
        'vkontakte' AS source,
        SUM(clicks) AS clicks,
        SUM(impressions) AS impressions,
        NULL AS installs,
        SUM(costs) AS costs
    FROM {{ ref('stg_vkontakte') }}
    WHERE is_realweb AND NOT is_ret_campaign
    GROUP BY 1, 2, 3, 4
),
yandex AS (
    SELECT 
        date, 
        campaign_name,
        adset_name,
        platform,
        'yandex' AS source,
        SUM(clicks) AS clicks,
        SUM(impressions) AS impressions,
        NULL AS installs,
        SUM(costs) AS costs
    FROM {{ ref('stg_yandex') }}
    WHERE is_realweb AND NOT is_ret_campaign
    GROUP BY 1, 2, 3, 4
),
af_installs AS (
    SELECT 
        date, 
        campaign_name,
        adset_name,
        platform,
        source,
        COUNT(appsflyer_id) AS installs
    FROM {{ ref('stg_af_installs') }}
    WHERE is_realweb AND NOT is_ret_campaign AND source!='other'
    GROUP BY 1, 2, 3, 4, 5
),
all_sources AS (
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
)

SELECT
    date, 
    campaign_name,
    adset_name,
    platform,
    source,
    COALESCE(clicks, 0) AS clicks,
    COALESCE(impressions, 0) AS impressions,
    COALESCE(COALESCE(a.installs, i.installs), 0) AS installs,
    COALESCE(costs, 0) AS costs
FROM all_sources AS a
FULL OUTER JOIN af_installs AS i
USING(date, campaign_name, adset_name, platform, source)	      
WHERE 
    platform='android' OR platform='ios'
    AND campaign_name IS NOT NULL
