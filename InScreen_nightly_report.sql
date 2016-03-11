SELECT t.created, t.userMarketId, t.adspaceId, t.adId, t.clientVersionId
, t.delivery, t.deliveredImp, t.measurableImp, t.viewableImp, t.monetisedImp, t.click, t.firstInteraction
, t.inscreen_pct_00_vt_00, t.inscreen_pct_30_vt_300, t.inscreen_pct_30_vt_3000, t.inscreen_pct_50_vt_1000
FROM 
(
    SELECT '2015-12-20 10:00:00' AS created
    , d.user_marketid AS userMarketId
    , d.adSpace_adSpaceId AS adSpaceId
    , d.ad_adId AS adId
    , NVL(d.client_clientversionid,0) AS clientVersionId
    , COUNT(DISTINCT CASE WHEN d.valid = 1 AND d.timestamp >= '2015-12-20 10:00:00' AND d.timestamp < '2015-12-20 11:00:00' THEN d.deliveryid END) AS delivery
    , COUNT(DISTINCT CASE WHEN i.impressiontype = 0 THEN i.impressionid END) AS deliveredImp
    , COUNT(DISTINCT CASE WHEN i.impressiontype = 0 AND (d.capabilitiesmask & 4294967296) > 0 THEN i.impressionid END) AS measurableImp
    , COUNT(DISTINCT CASE WHEN i.impressiontype = 1 THEN i.impressionid END) AS viewableImp
    , COUNT(DISTINCT CASE WHEN i.isimpression = 1 THEN i.impressionid END) AS monetisedImp 
    , COUNT(DISTINCT c.clickid) AS click
    , COUNT(DISTINCT CASE WHEN e.eventtype = 4 THEN e.eventid END) AS firstInteraction 
    , COUNT(DISTINCT CASE WHEN d.valid = 1 AND inq2.inscreen_pct_00_vt >= 0 THEN d.deliveryId END) AS inscreen_pct_00_vt_00
    , COUNT(DISTINCT CASE WHEN d.valid = 1 AND inq2.inscreen_pct_30_vt >= 300 THEN d.deliveryId END) AS inscreen_pct_30_vt_300
    , COUNT(DISTINCT CASE WHEN d.valid = 1 AND inq2.inscreen_pct_30_vt >= 3000 THEN d.deliveryId END) AS inscreen_pct_30_vt_3000 
    , COUNT(DISTINCT CASE WHEN d.valid = 1 AND inq2.inscreen_pct_50_vt >= 1000 THEN d.deliveryId END) AS inscreen_pct_50_vt_1000
    FROM public.rl2_delivery d
    LEFT OUTER JOIN 
    (
        SELECT inq1.deliveryId
        , inq1.adId
        , SUM(CASE WHEN inq1.sizepct >= 0 THEN incremental_TimeMs END) AS inscreen_pct_00_vt
        , SUM(CASE WHEN inq1.sizepct >= 30 THEN incremental_TimeMs END) AS inscreen_pct_30_vt
        , SUM(CASE WHEN inq1.sizepct >= 50 THEN incremental_TimeMs END) AS inscreen_pct_50_vt
        FROM 
        (
            SELECT i.deliveryId
            , i.adId
            , i.sizepct
            , sum(i.timems) AS incremental_TimeMs
            FROM public.inscreen_currentposition i
            WHERE i.timestamp >= '2015-12-20 10:00:00' AND i.timestamp < '2015-12-20 11:00:00'
            GROUP BY i.deliveryId, i.adId, i.sizepct
        ) inq1
        GROUP BY inq1.deliveryId, inq1.adId
    ) inq2 ON (d.deliveryId = inq2.deliveryId AND d.ad_adId = inq2.adId)
	LEFT OUTER JOIN public.rl2_impression i ON (d.deliveryid = i.deliveryid AND i.timestamp >= '2015-12-20 10:00:00' AND i.timestamp < '2015-12-20 11:00:00' AND i.valid = 1)
	LEFT OUTER JOIN public.rl2_click c ON (d.deliveryid = c.deliveryid AND c.timestamp >= '2015-12-20 10:00:00' AND c.timestamp < '2015-12-20 11:00:00' AND c.valid = 1)
	LEFT OUTER JOIN public.rl2_event e ON (d.deliveryid = e.deliveryid AND e.timestamp >= '2015-12-20 10:00:00' AND e.timestamp < '2015-12-20 11:00:00' AND e.valid = 1)
	WHERE d.timestamp >= '2015-12-20 06:00:00' AND d.timestamp < '2015-12-20 11:00:00'
	AND d.user_marketid IS NOT NULL
	AND d.noad = 0
    GROUP BY d.user_marketid, d.adSpace_adSpaceId, d.ad_adId, NVL(d.client_clientversionid,0)
) t
WHERE t.delivery > 0 OR t.deliveredImp > 0 OR t.measurableImp > 0 OR t.viewableImp > 0 OR t.monetisedImp > 0 OR t.click > 0 OR t.firstInteraction > 0 
;

-- 1. the calculation process of total time and in-screen percentage. We need confirmation about the logic . Ans: We got this info from Shao. 
-- 2. where are the other metric columns of the old in-screen report table? Ans: Since other metric columns are not used We got this info from Shao. 
-- 3. we should also add the conditions for the in-screen related metric columns like t.inscreen_pct_00_vt_00 > 0. Ans: already added.
-- 4. Is our current problem solved from this new script? Ans: Yes.