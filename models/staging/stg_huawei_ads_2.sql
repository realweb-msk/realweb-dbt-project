SELECT h.date,
       h.campaign_name,
       i.adset_name,
       h.platform,
       h.ad_source,
       h.impressions_sum,
       h.clicks_sum,
       IFNULL(i.installs, 0) AS installs_sum,
       h.costs_sum
FROM {{ ref("stg_huawei_ads_1_2") }} AS h
LEFT JOIN {{ ref("stg_huawei_ads_1_1" )}} AS i
    ON h.date = i.date AND
       h.campaign_name = i.campaign_name