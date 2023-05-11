WITH 
-- рекламные компании, в которых есть данные об установках
tiktok AS (SELECT date, 
                campaign_name, 
                adset_name, 
                platform, 
                SUM(clicks) AS clicks, 
                SUM(impressions) AS impressions, 
                SUM(costs) AS costs
        FROM {{ ref('stg_tiktok') }}
        WHERE (platform LIKE '%ios%' OR platform LIKE '%android%') AND is_realweb=TRUE AND is_ret_campaign=FALSE 
        GROUP BY date, campaign_name, adset_name, platform
        ORDER BY date),         

filtered_table AS (SELECT date,
                        campaign_name, 
                        COUNT(appsflyer_id) as installs
                FROM {{ ref('stg_af_installs')}} 
                WHERE campaign_name IS NOT NULL 
                     AND is_realweb=TRUE 
                     AND is_ret_campaign=FALSE 
                     AND source!='other'
                GROUP BY date, campaign_name
                ORDER BY date)
-- теперь проведем окончательную фильстраци данных и добавление кол-ва установок там, где это необходимо
-- зададим campaign_name в соответствии с текущей таблицей
/* используем пересечение данных - filtered_table хранит все отфильтрованные рекламные компании, 
нам же нужно оставить в конечной таблице с конкретной рекламной компанией только те, которые есть в filtered_table */
SELECT f.date,
                         REPLACE(f.campaign_name, f.campaign_name, 'tiktok') AS campaign_name,
                        t.adset_name, 
                        t.platform, 
                        SUM(t.clicks) AS clicks, 
                        SUM(t.impressions) AS impressions, 
                        SUM(t.costs) AS costs,
                        SUM(f.installs) AS installs
                FROM tiktok AS t INNER JOIN filtered_table AS f ON (t.date=f.date AND t.campaign_name=f.campaign_name)
                GROUP BY f.date, f.campaign_name, t.adset_name, t.platform
                ORDER BY f.date, f.campaign_name