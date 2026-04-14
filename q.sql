SELECT *
FROM dim_lead AS d
JOIN fact_leadhistory AS f
ON d.sk_lead = f.sk_lead
LIMIT 5;
