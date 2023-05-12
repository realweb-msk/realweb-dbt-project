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

WITH

facebook AS
(SELECT
date,
campaign_name,
adset_name,
platform,
'facebook' AS source,
clicks,
impressions,
installs,
costs
FROM
{{ ref('stg_facebook') }}
WHERE
is_realweb=TRUE
AND is_ret_campaign=FALSE),

google AS (SELECT
date,
campaign_name,
adset_name,
platform,
'google_ads' AS source,
clicks,
impressions,
installs,
costs
FROM
{{ ref('stg_google_ads') }}
WHERE
is_realweb=TRUE
AND is_ret_campaign=FALSE),
 
af_by_camp AS
(SELECT 
date,
campaign_name,
platform,
source,
COUNT(appsflyer_id) AS installs,
FROM
{{ ref('stg_af_installs') }}
WHERE
is_realweb=TRUE
AND is_ret_campaign=FALSE
AND source IN ('huawei','mytarget','vkontakte','yandex')
GROUP BY
date,
campaign_name,
platform,
source),

huawei_raw AS
(SELECT 
date,
campaign_name,
'not defined' AS adset_name, 
platform,
'huawei' AS source,
IFNULL(clicks, 0) AS clicks,
IFNULL(costs, 0) AS costs,
IFNULL(impressions, 0) AS impressions
FROM
{{ ref('stg_huawei_ads') }}
WHERE
is_realweb=TRUE
AND is_ret_campaign=FALSE),

huawei AS 
(SELECT
hw.date,
hw.campaign_name,
hw.adset_name, 
hw.platform,
hw.source,
hw.clicks,
hw.costs,
IFNULL(af.installs, 0) AS installs,
hw.impressions
FROM huawei_raw AS hw
LEFT JOIN af_by_camp AS af ON
hw.date=af.date AND
hw.campaign_name=af.campaign_name),

mytarget_raw AS
(SELECT 
date,
campaign_name,
'not defined' AS adset_name,
platform,
'mytarget' AS source,
IFNULL(clicks, 0) AS clicks,
IFNULL(costs, 0) AS costs,
IFNULL(impressions, 0) AS impressions
FROM
{{ ref('stg_mytarget') }}
WHERE
is_realweb=TRUE
AND is_ret_campaign=FALSE),

mytarget AS
(SELECT
mt.date,
mt.campaign_name,
mt.adset_name, 
mt.platform,
mt.source,
mt.clicks,
mt.costs,
IFNULL(af.installs, 0) AS installs,
mt.impressions
FROM mytarget_raw AS mt
LEFT JOIN af_by_camp AS af ON
mt.date=af.date AND
mt.campaign_name=af.campaign_name AND
mt.source=af.source),

vkontakte_raw AS
(SELECT 
date,
campaign_name,
'not defined' AS adset_name,
platform,
'vkontakte' AS source,
IFNULL(clicks, 0) AS clicks,
IFNULL(costs, 0) AS costs,
IFNULL(impressions, 0) AS impressions
FROM
{{ ref('stg_vkontakte') }}
WHERE
is_realweb=TRUE
AND is_ret_campaign=FALSE),

vkontakte AS
(SELECT
vk.date,
vk.campaign_name,
vk.adset_name, 
vk.platform,
vk.source,
vk.clicks,
vk.costs,
IFNULL(af.installs, 0) AS installs,
vk.impressions
FROM vkontakte_raw AS vk
LEFT JOIN af_by_camp AS af ON
vk.date=af.date AND
vk.campaign_name=af.campaign_name AND
vk.source=af.source),

yandex_raw AS
(SELECT 
date,
campaign_name,
'not defined' AS adset_name,
platform,
'yandex' AS source,
IFNULL(SUM(clicks),0) AS clicks,
IFNULL(SUM(costs),0) AS costs,
IFNULL(SUM(impressions),0) AS impressions
FROM
{{ ref('stg_yandex') }}
WHERE
is_realweb=TRUE
AND is_ret_campaign=FALSE
GROUP BY
date,
campaign_name,
adset_name,
platform,
source),

yandex AS
(SELECT
ya.date,
ya.campaign_name,
ya.adset_name, 
ya.platform,
ya.source,
ya.clicks,
ya.costs,
IFNULL(af.installs, 0) AS installs,
ya.impressions
FROM yandex_raw AS ya
LEFT JOIN af_by_camp AS af ON
ya.date=af.date AND
ya.campaign_name=af.campaign_name AND
ya.source=af.source),

af_by_adset AS
(SELECT 
date,
campaign_name,
adset_name,
platform,
source,
COUNT(appsflyer_id) AS installs,
FROM
{{ ref('stg_af_installs') }}
WHERE
is_realweb=TRUE
AND is_ret_campaign=FALSE
AND source='tiktok'
GROUP BY
date,
campaign_name,
adset_name,
platform,
source),

tiktok_raw AS
(SELECT 
date,
campaign_name,
adset_name,
platform,
'tiktok' AS source,
IFNULL(clicks, 0) AS clicks,
IFNULL(costs, 0) AS costs,
IFNULL(impressions, 0) AS impressions
FROM
{{ ref('stg_tiktok') }}
WHERE
is_realweb=TRUE
AND is_ret_campaign=FALSE),

tiktok AS
(SELECT
tik.date,
tik.campaign_name,
tik.adset_name, 
tik.platform,
tik.source,
tik.clicks,
tik.costs,
IFNULL(af.installs, 0) AS installs,
tik.impressions
FROM tiktok_raw AS tik
LEFT JOIN af_by_adset AS af ON
tik.date=af.date AND
tik.campaign_name=af.campaign_name AND
tik.adset_name=af.adset_name)

SELECT * FROM
(SELECT * from facebook
UNION ALL
SELECT * from google
UNION ALL
SELECT * from huawei
UNION ALL
SELECT * from mytarget
UNION ALL
SELECT * from tiktok
UNION ALL
SELECT * from vkontakte
UNION ALL
SELECT * from yandex)