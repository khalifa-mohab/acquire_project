-- Aggregate session data by date and platform for traffic sources coming from 'cpc' (cost-per-click)
WITH sessions_data AS (
  SELECT
    date,
    traffic_source AS platform,
    COUNT(DISTINCT session_id) AS session_clicks
  FROM {{ source('prism_acquire', 'sessions') }}
  WHERE traffic_medium = 'cpc'
  GROUP BY date, platform
),

-- Aggregate transaction data by date and platform
transactions_data AS (
  SELECT
    s.date,
    s.traffic_source AS platform,
    COUNT(DISTINCT t.transaction_id) AS transactions,
    SUM(t.transaction_total) AS total_revenue
  FROM {{ source('prism_acquire', 'transactions') }} t
  INNER JOIN {{ source('prism_acquire', 'sessions') }} s
  ON t.session_id = s.session_id
  GROUP BY s.date, s.traffic_source
)

-- Combine ad data with session and transaction data to calculate key metrics
SELECT
  ad_data.date,
  ad_data.platform,
  ad_data.clicks AS total_clicks,
  ad_data.impressions AS total_impressions,
  sessions_data.session_clicks AS total_sessions,
  transactions_data.transactions AS total_transactions,
  ad_data.cost AS total_cost,
  ROUND((ad_data.clicks / ad_data.impressions) * 100, 2) AS CTR,
  ROUND((ad_data.cost / transactions_data.transactions), 2) AS CPA,
  ROUND((transactions_data.transactions / sessions_data.session_clicks) * 100, 2) AS CVR,
  ROUND((ad_data.cost / (ad_data.impressions / 1000)), 2) AS CPM
FROM
  {{ ref('unpivoted_adplatform_data') }} ad_data
INNER JOIN --Join with session data
  sessions_data
ON
  ad_data.date = sessions_data.date AND ad_data.platform = sessions_data.platform
LEFT JOIN -- Join with transaction data
  transactions_data
ON
  ad_data.date = transactions_data.date AND ad_data.platform = transactions_data.platform
ORDER BY
  ad_data.date, ad_data.platform

{{ config(materialized='table') }}