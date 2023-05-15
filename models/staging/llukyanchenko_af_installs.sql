{{ config(materialized='table') }}

WITH 
    sources as(
                {{add_source('stg_yandex')}}
                UNION ALL 
                {{add_source('stg_vkontakte')}}
                UNION ALL
                {{add_source('stg_tiktok')}}
                UNION ALL 
                {{add_source('stg_mytarget')}}
                UNION ALL
                (   SELECT 
                        date,
                        campaign_name,
                        'huawei_ads' as adset_name,
                        platform,
                        'huawei' as source,
                        SUM(clicks) as clicks,
                        SUM(impressions) as impressions,
                        SUM(costs) as costs,
                        NULL as installs
                    FROM {{ref('stg_huawei_ads')}}
                    WHERE 
                        is_realweb 
                        AND NOT is_ret_campaign
                    GROUP BY 
                        date,
                        campaign_name,
                        platform
                    )
                    UNION ALL
                    (
                        SELECT date,
                        campaign_name,
                        adset_name,
                        platform,
                        'facebook' as source,
                        SUM(clicks) as clicks,
                        SUM(impressions) as impressions,
                        SUM(costs) as costs,
                        SUM(installs) as installs
                    FROM {{ref('stg_facebook')}}
                    WHERE 
                        is_realweb 
                        AND NOT is_ret_campaign
                    GROUP BY 
                        date,
                        campaign_name,
                        adset_name,
                        platform
                    )
                    UNION ALL
                    (
                        SELECT date,
                        campaign_name,
                        adset_name,
                        platform,
                        'facebook' as source,
                        SUM(clicks) as clicks,
                        SUM(impressions) as impressions,
                        SUM(costs) as costs,
                        SUM(installs) as installs
                    FROM {{ref('stg_google_ads')}}
                    WHERE 
                        is_realweb 
                        AND NOT is_ret_campaign
                    GROUP BY 
                        date,
                        campaign_name,
                        adset_name,
                        platform
                    )

                ),
    installs AS (
        {{installs_counter('stg_af_installs')}}
    ),
    summary AS(
        SELECT
            COALESCE(s.date, i.date) AS date,
            COALESCE(s.campaign_name, i.campaign_name) AS campaign_name,
            COALESCE(s.adset_name, i.adset_name) AS adset_name,
            COALESCE(s.platform, i.platform) AS platform,
            COALESCE(s.source, i.source) AS source,
            COALESCE(clicks, 0) AS clicks,
            COALESCE(impressions, 0) AS impressions,
            COALESCE(costs, 0) AS costs,
            COALESCE(s.installs, i.installs, 0) AS installs
        FROM sources AS s  
        FULL OUTER JOIN installs AS i ON s.date = i.date   
                                      AND s.campaign_name = i.campaign_name
                                      AND s.adset_name = i.adset_name
                                      AND s.platform = i.platform
                                      AND s.source = i.source
    )

SELECT *
FROM summary
WHERE (platform = 'ios' OR platform = 'android')
AND impressions + clicks + costs + installs > 0
AND source != 'other'
ORDER BY date

