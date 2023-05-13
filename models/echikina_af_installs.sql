WITH facebook AS (
    SELECT
        date,
        campaign_name,
        adset_name,
        platform,
        'facebook' AS source,
        clicks,
        costs,
        installs,
        impressions
FROM `realweb-152714`.`dbt_echikina`.`stg_facebook`
WHERE is_realweb AND NOT is_ret_campaign
),

google_ads AS (
    SELECT
        date,
        campaign_name,
        adset_name,
        platform,
        'google_ads' AS source,
        clicks,
        costs,
        installs,
        impressions
FROM `realweb-152714`.`dbt_echikina`.`stg_google_ads`
WHERE is_realweb AND NOT is_ret_campaign
),

huawei AS (
    SELECT
        date,
        campaign_name,
        '' AS adset_name,
        platform,
        'huawei_ads' AS source,
        clicks,
        costs,
        NULL AS installs,
        impressions
FROM `realweb-152714`.`dbt_echikina`.`stg_huawei_ads`
WHERE is_realweb AND NOT is_ret_campaign
),

mytarget AS (
    SELECT
        date,
        campaign_name,
        adset_name,
        platform,
        'mytarget' AS source,
        clicks,
        costs,
        NULL AS installs,
        impressions
FROM `realweb-152714`.`dbt_echikina`.`stg_mytarget`
WHERE is_realweb AND NOT is_ret_campaign 
),

tiktok AS (
    SELECT
        date,
        campaign_name,
        adset_name,
        platform,
        'tiktok' AS source,
        clicks,
        costs,
        NULL AS installs,
        impressions
FROM `realweb-152714`.`dbt_echikina`.`stg_tiktok`
WHERE is_realweb AND NOT is_ret_campaign 
),

vk AS (
    SELECT
        date,
        campaign_name,
        adset_name,
        platform,
        'vkontakte' AS source,
        clicks,
        costs,
        NULL AS installs,
        impressions
FROM `realweb-152714`.`dbt_echikina`.`stg_vkontakte`
WHERE is_realweb AND NOT is_ret_campaign 
),

yandex AS (
    SELECT
        date,
        campaign_name,
        adset_name,
        platform,
        'yandex' AS source,
        clicks,
        costs,
        NULL AS installs,
        impressions
FROM `realweb-152714`.`dbt_echikina`.`stg_yandex`
WHERE is_realweb AND NOT is_ret_campaign
),

all_sources AS ( 
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
    SELECT * FROM vk
    UNION ALL 
    SELECT * FROM yandex
),

not_attr_installs AS (
    SELECT 
        date,
        campaign_name,
        adset_name,
        platform,
        source,
        COUNT(DISTINCT appsflyer_id) AS installs
    FROM `realweb-152714`.`dbt_echikina`.`stg_af_installs`
    WHERE is_realweb AND NOT is_ret_campaign
    GROUP BY 1, 2, 3, 4, 5
),

final AS (
    SELECT       
        al.date,
        IFNULL(al.campaign_name, na.campaign_name) AS campaign_name,
        al.adset_name,
        al.platform,
        al.source,
        IFNULL(al.installs, na.installs) AS installs,
        impressions,
        clicks,
        costs
    FROM all_sources as al
    FULL JOIN not_attr_installs as na
    ON al.date = na.date
        and al.campaign_name = na.campaign_name
        and al.adset_name = na.adset_name
        and al.platform = na.platform
        and al.source = na.source
)

SELECT
    date,
    SUM(clicks) AS clicks,
    SUM(costs) AS costs,
    SUM(installs) AS installs,
    SUM(impressions) AS impressions
FROM final
WHERE source != 'other'
GROUP BY date, campaign_name, adset_name, platform, source
