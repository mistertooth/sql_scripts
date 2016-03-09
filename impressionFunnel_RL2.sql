-- impression funnel on the campaign
SELECT 
COUNT(DISTINCT CASE WHEN d.valid = 1 AND noad = 0 THEN d.deliveryid END) AS delivery,
COUNT(DISTINCT CASE WHEN i.valid = 1 AND i.impressiontype = 0 THEN d.deliveryid END) AS deliveredImp,
COUNT(DISTINCT CASE WHEN i.valid = 1 AND i.impressiontype = 0 AND (capabilitiesmask & 4294967296) > 0 THEN d.deliveryid END) AS measurableImp_RL2,
COUNT(DISTINCT CASE WHEN i.valid = 1 AND i.impressiontype = 1 THEN d.deliveryid END) AS viewableImp_RL2,
COUNT(DISTINCT CASE WHEN i.valid= 1 AND noad = 0 AND i.isimpression = 1 THEN d.deliveryid END) AS monetizedImp_RL2
FROM public.rl2_delivery d 
INNER JOIN public.rl2_impression i ON (d.deliveryid=i.deliveryid) 
WHERE d.ad_adid in (38768)
AND  i.valid = 1
AND d.timestamp >= '2015-12-19 00:00:00' AND d.timestamp < '2016-01-01 00:00:00' 
;