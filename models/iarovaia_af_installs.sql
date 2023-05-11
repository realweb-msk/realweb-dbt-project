/* Путь, которым я шла к конечной модели мне кажется довольно дорогим и не оптимальным, 
т.к приходится каждую таблицу рекламного кабинета join-ить с таблицей stg_af_installs.
Как минимум потому, что даже для тех таблиц рекламных кабинетов, 
где есть вся необходимая информация вплоть до установок нужна дополнительная проверка на source!='other', 
которую можно сделать только через эту большую таблицу (причем эта фильтрация убирает достаточно много данных)

За основу я брала таблицы с рекламными компаниями.
И постаралась максимально оптимизировать процесс, насколько смогла, 
но при этом выполняя работу поэтапно для минимизации ошибок и понимания происходящего -
join-ила не все таблицы сразу, а предварительно фильтровала их по нужным нам данным. 
В конкретно этом случае я не увидела особой разницы в размерах исходных\фильтрованных таблиц, 
но в другом случае это, возможно, принесло бы больше пользы) */


-- {{config(materialized='table')}}

-- отфильтруем таблицы рекламных компаний под задачу
-- сразу проводим фильтрацию по значению платформы, только кампании Риалвеба и только User Acquisition
-- затем отфильтруем таблицу stg_af_installs и используем ее для вывода installs и фильтрации по source!='other'

WITH 
-- рекламные компании, в которых есть данные об установках
facebook AS (SELECT date, 
                    campaign_name, 
                    adset_name, 
                    platform, 
                    SUM(clicks) AS clicks, 
                    SUM(impressions) AS impressions, 
                    SUM(costs) AS costs, 
                    SUM(installs) AS installs
            FROM {{ ref('stg_facebook') }}
            WHERE (platform LIKE '%ios%' OR platform LIKE '%android%') AND is_realweb=TRUE AND is_ret_campaign=FALSE 
            GROUP BY date, campaign_name, adset_name, platform
            ORDER BY date),
goolge AS (SELECT date, 
                    campaign_name, 
                    adset_name, 
                    platform, 
                    SUM(clicks) AS clicks, 
                    SUM(impressions) AS impressions, 
                    SUM(costs) AS costs, 
                    SUM(installs) AS installs
            FROM {{ ref('stg_google_ads') }}
            WHERE (platform LIKE '%ios%' OR platform LIKE '%android%') AND is_realweb=TRUE AND is_ret_campaign=FALSE 
            GROUP BY date, campaign_name, adset_name, platform
            ORDER BY date),
-- рекламные компании, в которых нет данных об установках
huawei AS (SELECT date, 
                campaign_name, 
                platform, 
                SUM(clicks) AS clicks, 
                SUM(impressions) AS impressions, 
                SUM(costs) AS costs
        FROM {{ ref('stg_huawei_ads') }}
        WHERE (platform LIKE '%ios%' OR platform LIKE '%android%') AND is_realweb=TRUE AND is_ret_campaign=FALSE 
        GROUP BY date, campaign_name, platform
        ORDER BY date),
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
vk AS (SELECT date, 
                campaign_name, 
                adset_name, 
                platform, 
                SUM(clicks) AS clicks, 
                SUM(impressions) AS impressions, 
                SUM(costs) AS costs
        FROM {{ ref('stg_vkontakte') }}
        WHERE (platform LIKE '%ios%' OR platform LIKE '%android%') AND is_realweb=TRUE AND is_ret_campaign=FALSE 
        GROUP BY date, campaign_name, adset_name, platform
        ORDER BY date),
yandex AS (SELECT date, 
                campaign_name, 
                adset_name, 
                platform, 
                SUM(clicks) AS clicks, 
                SUM(impressions) AS impressions, 
                SUM(costs) AS costs
        FROM {{ ref('stg_yandex') }}
        WHERE (platform LIKE '%ios%' OR platform LIKE '%android%') AND is_realweb=TRUE AND is_ret_campaign=FALSE 
        GROUP BY date, campaign_name, adset_name, platform
        ORDER BY date),
-- фильтруем таблицу stg_af_installs
filtered_table AS (SELECT date,
                        campaign_name, 
                        adset_name,
                        COUNT(appsflyer_id) as installs
                FROM {{ ref('stg_af_installs')}} 
                WHERE campaign_name IS NOT NULL 
                     AND is_realweb=TRUE 
                     AND is_ret_campaign=FALSE 
                     AND source!='other'
                GROUP BY date, campaign_name, adset_name
                ORDER BY date),
-- теперь проведем окончательную фильстраци данных и добавление кол-ва установок там, где это необходимо
-- зададим campaign_name в соответствии с текущей таблицей
/* используем пересечение данных - filtered_table хранит все отфильтрованные рекламные компании, 
нам же нужно оставить в конечной таблице с конкретной рекламной компанией только те, которые есть в filtered_table */
final_facebook AS (SELECT f.date,
                        fb.campaign_name,
                        fb.adset_name, 
                        fb.platform, 
                        REPLACE(fb.campaign_name, fb.campaign_name, 'facebook') AS advertising_cabinet,
                        SUM(fb.clicks) AS clicks, 
                        SUM(fb.impressions) AS impressions, 
                        SUM(fb.costs) AS costs,
                        SUM(fb.installs) AS installs
                -- объединяем по совпадению даты и названию кампании        
                FROM facebook AS fb INNER JOIN filtered_table AS f ON (fb.date=f.date AND fb.campaign_name=f.campaign_name)
                GROUP BY f.date, fb.campaign_name, fb.adset_name, fb.platform, advertising_cabinet
                ORDER BY f.date, fb.campaign_name),
final_google AS (SELECT f.date,
                        g.campaign_name,
                        g.adset_name, 
                        g.platform, 
                        REPLACE(g.campaign_name, g.campaign_name, 'google') AS advertising_cabinet,
                        SUM(g.clicks) AS clicks, 
                        SUM(g.impressions) AS impressions, 
                        SUM(g.costs) AS costs,
                        SUM(g.installs) AS installs
                -- объединяем по совпадению даты и названию кампании        
                FROM goolge AS g INNER JOIN filtered_table AS f ON (g.date=f.date AND g.campaign_name=f.campaign_name)
                GROUP BY f.date, g.campaign_name, g.adset_name, g.platform, advertising_cabinet
                ORDER BY f.date, g.campaign_name),
final_huawei AS (SELECT f.date,
                        h.campaign_name, 
                        f.adset_name, 
                        h.platform, 
                        REPLACE(h.campaign_name, h.campaign_name, 'huawei') AS advertising_cabinet,
                        SUM(h.clicks) AS clicks, 
                        SUM(h.impressions) AS impressions, 
                        SUM(h.costs) AS costs,
                        SUM(f.installs) AS installs
                FROM huawei AS h INNER JOIN filtered_table AS f ON (h.date=f.date AND h.campaign_name=f.campaign_name)
                GROUP BY f.date, h.campaign_name, f.adset_name, h.platform, advertising_cabinet
                ORDER BY f.date, h.campaign_name),
final_tiktok AS (SELECT f.date,
                        t.campaign_name,
                        t.adset_name, 
                        t.platform, 
                        REPLACE(t.campaign_name, t.campaign_name, 'tiktok') AS advertising_cabinet,
                        SUM(t.clicks) AS clicks, 
                        SUM(t.impressions) AS impressions, 
                        SUM(t.costs) AS costs,
                        SUM(f.installs) AS installs
                FROM tiktok AS t INNER JOIN filtered_table AS f ON (t.date=f.date AND t.campaign_name=f.campaign_name)
                GROUP BY f.date, t.campaign_name, t.adset_name, t.platform, advertising_cabinet
                ORDER BY f.date, t.campaign_name),
final_vk AS (SELECT f.date,
                    vk.campaign_name, 
                    vk.adset_name, 
                    vk.platform, 
                    REPLACE(vk.campaign_name, vk.campaign_name, 'vk') AS advertising_cabinet,
                    SUM(vk.clicks) AS clicks, 
                    SUM(vk.impressions) AS impressions, 
                    SUM(vk.costs) AS costs,
                    SUM(f.installs) AS installs
                FROM vk INNER JOIN filtered_table AS f ON (vk.date=f.date AND vk.campaign_name=f.campaign_name)
                GROUP BY f.date, vk.campaign_name, vk.adset_name, vk.platform, advertising_cabinet
                ORDER BY f.date, vk.campaign_name),
final_yandex AS (SELECT f.date,
                        y.campaign_name, 
                    y.adset_name, 
                    y.platform,
                    REPLACE(y.campaign_name, y.campaign_name, 'yandex') AS advertising_cabinet, 
                    SUM(y.clicks) AS clicks, 
                    SUM(y.impressions) AS impressions, 
                    SUM(y.costs) AS costs,
                    SUM(f.installs) AS installs
                FROM yandex AS y INNER JOIN filtered_table AS f ON (y.date=f.date AND y.campaign_name=f.campaign_name)
                GROUP BY f.date, y.campaign_name, y.adset_name, y.platform, advertising_cabinet
                ORDER BY f.date, y.campaign_name)                                                              

-- теперь у нас есть отдельные отфильтрованные индентичные таблицы рекламных компаний
-- соединим и в одну таким образом, что порядок будет 1. по дате 2. по названию рекламной кампании

SELECT * FROM final_facebook
UNION ALL
SELECT * FROM final_google
UNION ALL
SELECT * FROM final_huawei
UNION ALL
SELECT * FROM final_tiktok
UNION ALL
SELECT * FROM final_vk
UNION ALL
SELECT * FROM final_yandex

