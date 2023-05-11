
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

-- Загружаем данные из AppsFlyer, считаем установки

WITH installs_table AS (
	SELECT
	    date,
	    campaign_name,
	    adset_name,
	    platform,
	    source,
	    COUNT(appsflyer_id) AS installs
	FROM {{ref('stg_af_installs')}}
	WHERE is_ret_campaign IS FALSE 
	AND is_realweb IS TRUE
	AND source != 'other'
	GROUP BY 
	    date,
	    campaign_name,
	    adset_name,
	    platform,
	    source
	),

-- Собираем данные по рекламным кабинетам

tiktok AS (
	SELECT
	    date,
	    campaign_name,
	    {{ adset_name_filter('adset_name') }} AS adset_name, -- заменяем значения '-' и '--', они всё равно не несут смысловой нагрузки, вообще странно, что значения отсутствуют в кабинетах или appsflyer, со сбором данных может какие-то проблемы
	    platform,
	    {{ install_source('campaign_name') }} AS source,
	    impressions,
	    clicks,
	    NULL AS installs, -- отсутствие данных по установкам тоже странно, но раз их нет добавляем null, чтобы колонки совпадали во всех таблицах и их можно было склеить
	    costs	    
	FROM {{ref('stg_tiktok')}}
	WHERE is_ret_campaign IS FALSE 
	AND is_realweb IS TRUE
	),

yandex AS (
	SELECT
	    date,
	    campaign_name,
	    {{ adset_name_filter('adset_name') }} AS adset_name,
	    platform,
	    {{ install_source('campaign_name') }} AS source,
	    impressions,
	    clicks,
	    NULL AS installs,
	    costs	    
	FROM {{ref('stg_yandex')}}
	WHERE is_ret_campaign IS FALSE 
	AND is_realweb IS TRUE
	),

mytarget AS (
	SELECT
	    date,
	    campaign_name,
	    {{ adset_name_filter('adset_name') }} AS adset_name,
	    platform,
	    {{ install_source('campaign_name') }} AS source,
	    impressions,
	    clicks,
	    NULL AS installs,
	    costs	    
	FROM {{ref('stg_mytarget')}}
	WHERE is_ret_campaign IS FALSE 
	AND is_realweb IS TRUE
	),

vk AS (
	SELECT
	    date,
	    campaign_name,
	    {{ adset_name_filter('adset_name') }} AS adset_name,
	    platform,
	    {{ install_source('campaign_name') }} AS source,
	    impressions,
	    clicks,
	    NULL AS installs,
	    costs	    
	FROM {{ref('stg_vkontakte')}}
	WHERE is_ret_campaign IS FALSE 
	AND is_realweb IS TRUE
	),

huawei AS (
	SELECT
    	date,
	    campaign_name,
	    '' AS adset_name, -- из каба вообще не грузятся данные об адсетах, добавим пустое значение
	    platform,
	    {{ install_source('campaign_name') }} AS source,	    
	    impressions,
	    clicks,
	    NULL AS installs,
	    costs	    
	FROM {{ref('stg_huawei_ads')}}
	WHERE is_ret_campaign IS FALSE 
	AND is_realweb IS TRUE
),

google AS (
	SELECT
    	date,
	    campaign_name,
	    {{ adset_name_filter('adset_name') }} AS adset_name,
	    platform,
	    {{ install_source('campaign_name') }} AS source,	    
	    impressions,
	    clicks,
	    installs,
	    costs	    
	FROM {{ref('stg_google_ads')}}
	WHERE is_ret_campaign IS FALSE 
	AND is_realweb IS TRUE
),

facebook AS (
	SELECT
    	date,
	    campaign_name,
	    {{ adset_name_filter('adset_name') }} AS adset_name,
	    platform,
	    {{ install_source('campaign_name') }} AS source,
	    impressions,
	    clicks,
	    installs,
	    costs	    
	FROM {{ref('stg_facebook')}}
	WHERE is_ret_campaign IS FALSE 
	AND is_realweb IS TRUE 
),


-- объединим данные из кабинетов в общую таблицу

union_tables AS (
	SELECT * FROM google
	UNION DISTINCT
	SELECT * FROM facebook
	UNION DISTINCT
	SELECT * FROM tiktok
	UNION DISTINCT
	SELECT * FROM mytarget
	UNION DISTINCT
	SELECT * FROM vk
	UNION DISTINCT
	SELECT * FROM yandex
	UNION DISTINCT
	SELECT * FROM huawei
	),


-- теперь присоединим информацию из AppsFlyer к данным из кабинетов, решила делать full join чтобы точно не потерять ничего
-- если в таблице из РК уже есть данные по установкам, то берём их, если нет – добавляем из таблицы AppsFlyer
-- в данных из AppsFlyer нет кликов, показов и трат, вместо null значений проставим 0

final_joined_table AS (
	SELECT
		date,
	    campaign_name,
	    adset_name,
	    platform,
	    source,
	    IFNULL(impressions, 0) AS impressions,
	    IFNULL(clicks, 0) AS clicks,
	    IFNULL(IF(ut.installs IS NOT NULL, ut.installs, it.installs), 0) AS installs,
	    IFNULL(costs, 0.0) AS costs
	FROM union_tables ut 
	FULL OUTER JOIN installs_table it
	USING (
		date,
	    campaign_name,
	    adset_name,
	    platform,
	    source
	    )
	)

SELECT *
FROM final_joined_table