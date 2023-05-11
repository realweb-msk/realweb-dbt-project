WITH FB AS (
    SELECT 
        date,
        campaign_name,
        adset_name,
        platform,
        {{ install_source('campaign_name') }} AS ad_source,
        SUM(impressions) AS impressions_sum,
        SUM(clicks) AS clicks_sum,
        SUM(installs) AS installs_sum,
        SUM(costs) AS costs_sum
    FROM {{ ref ('stg_facebook') }}
    WHERE   1=1 AND
        is_realweb = TRUE AND
        is_ret_campaign = FALSE AND
        {{ install_source('campaign_name') }} != 'other' AND
        {{ platform('platform') }} != 'no_platform'
    GROUP BY date,
        campaign_name,
        adset_name,
        platform,
        ad_source
            )
SELECT * FROM FB