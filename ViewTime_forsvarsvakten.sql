
--OSE002723 Försvarsmakten Officer Takeover -Standard Viewable (CPMV)

-- viewable percentile distribution of total viewtime per impression
SELECT c.percentile*10 as percentile, max(c.vt) as ViewTimeMax, 
c.percentile*10-10 as percentile, min(c.vt) as ViewTimeMin
FROM
(SELECT b.VT,
ntile(10) OVER (order by b.VT) as percentile
FROM
(SELECT 
a.deliveryId,
sum(a.VT) as VT
FROM
(SELECT 
deliveryId, -- refreqlogid
sizepct, -- pctInscreen
sum(timems) as VT --incrementalTimeMs
FROM 
inscreen_shao.currentposition
WHERE 
adid in(35772,35874)
GROUP BY
deliveryId,
sizepct) a
WHERE a.sizepct>=30
GROUP BY a.deliveryId HAVING sum(a.VT)>=300
ORDER BY 2) b ) c
GROUP BY 1
ORDER BY 1;

#median 4635

-- measurable percentile distribution
SELECT c.percentile*10 as percentile, max(c.vt) as ViewTimeMax, 
c.percentile*10-10 as percentile, min(c.vt) as ViewTimeMin
FROM
(SELECT CASE WHEN b.VT IS NULL OR b.VT=0 THEN 0 ELSE b.VT END as vt,
ntile(10) OVER (order by CASE WHEN b.VT IS NULL OR b.VT=0 THEN 0 ELSE b.VT END) as percentile
FROM
(SELECT
d.deliveryId,
sum(c.timems) as VT
FROM public.rl2_delivery d 
INNER JOIN public.rl2_impression i ON (d.deliveryid=i.deliveryid)
FULL OUTER JOIN  inscreen_shao.currentposition c ON (c.deliveryid=d.deliveryid AND c.adid in (35772,35874))
WHERE d.ad_adid in (35772,35874)
AND  i.valid = 1 AND i.impressiontype = 0 AND (capabilitiesmask & 4294967296) > 0
AND d.timestamp >= '2015-11-01 00:00:00' AND d.timestamp < '2015-11-23 00:00:00' 
GROUP BY
1
ORDER BY 1) b ) c
GROUP BY 1
ORDER BY 1;

#median 1729

-- median uplift
# 168%

-- 3 avg view time by viewableImp
SELECT avg(b.VT) as avgViewTimeMS, count(distinct b.deliveryid) as viewableImp
FROM
(SELECT 
a.deliveryId,
sum(a.VT) as VT
FROM
(SELECT 
deliveryId, --refreqlogid
sizepct, --inscreenPct
sum(timems) as VT --incrementalTimeMs
FROM 
inscreen_shao.currentposition
WHERE 
adid in(35772,35874)
GROUP BY
deliveryId,
sizepct) a
WHERE a.sizepct>=30
GROUP BY a.deliveryId HAVING sum(a.VT)>=300) b 
;

# 14754	5106686

SELECT avg(b.VT) avgViewTimeMS, count(distinct b.deliveryid) as InscreenImp
FROM
(SELECT 
a.deliveryId,
sum(a.VT) as VT
FROM
(SELECT 
deliveryId, --refreqlogid
sizepct, --inscreenPct
sum(timems) as VT --incrementalTimeMs
FROM 
inscreen_shao.currentposition
WHERE 
adid in(35772,35874)
GROUP BY
deliveryId,
sizepct) a
WHERE a.sizepct>0
GROUP BY a.deliveryId ) b ;

# 16777	6053622

-- measurable imp from rl2
SELECT 
COUNT(DISTINCT CASE WHEN i.valid = 1 AND i.impressiontype = 0 AND (capabilitiesmask & 4294967296) > 0 THEN d.deliveryid END) AS measurableImp_RL2
FROM public.rl2_delivery d 
INNER JOIN public.rl2_impression i ON (d.deliveryid=i.deliveryid) 
WHERE d.ad_adid in (35772,35874)
AND  i.valid = 1
AND d.timestamp >= '2015-11-01 00:00:00' AND d.timestamp < '2015-11-23 00:00:00' 
;

# 3134760

-- inscreen from inscreenlog way bigger than measurable from requestlog, so no hack on the avg uplift like HP campaign


--number of imps per sec from first 50 seconds & plot a bar chart from the result
SELECT round(b.VT/1000) as viewTimeSec, count(distinct deliveryId) as numImps
FROM
(SELECT 
a.deliveryId,
sum(a.VT) as VT
FROM
(SELECT 
deliveryId, --refreqlogid
sizepct, --inscreenPct
sum(timems) as VT --incrementalTimeMs
FROM 
inscreen_shao.currentposition
WHERE 
adid in(35772,35874)
GROUP BY
deliveryId,
sizepct) a
WHERE a.sizepct>=30
GROUP BY a.deliveryId HAVING sum(a.VT)>=300) b
group by 1
order by 1
limit 51 ;


--percentile viewtime by apptype

SELECT c.apptype, c.percentile*10 as percentile, max(c.vt) as ViewTimeMax
FROM
(SELECT b.apptype,b.VT,
ntile(10) OVER (order by b.VT) as percentile
FROM
(SELECT 
a.apptype,
a.deliveryId,
sum(a.VT) as VT
FROM
(SELECT 
s.apptype,
deliveryId, -- refreqlogid
sizepct, -- pctInscreen
sum(timems) as VT --incrementalTimeMs
FROM 
inscreen_shao.currentposition i
INNER JOIN
public.novadb_online_adspace s 
ON i.sid=s.extsiteid
WHERE 
adid in(35772,35874)
GROUP BY
s.apptype,
deliveryId,
sizepct) a
WHERE a.sizepct>=30
GROUP BY a.apptype,a.deliveryId HAVING sum(a.VT)>=300
ORDER BY 3) b ) c
GROUP BY 1,2
ORDER BY 1,2;


-- not needed for reports
-- percentile distribution on avg view time per viewable impression
SELECT c.percentile*10 as percentile, max(c.vt_avg) as ViewTimeMax, 
c.percentile*10-10 as percentile, min(c.vt_avg) as ViewTimeMin
FROM
(SELECT d.vt_avg,
ntile(10) OVER (order by d.vt_avg) as percentile
FROM
(SELECT 
a.deliveryId,
sum(a.VT) as VT
FROM
(SELECT 
deliveryId, -- refreqlogid
sizepct, -- pctInscreen
sum(timems) as VT --incrementalTimeMs
FROM 
inscreen_shao.currentposition
WHERE 
adid in(35772,35874)
GROUP BY
deliveryId,
sizepct) a
WHERE a.sizepct>=30
GROUP BY a.deliveryId HAVING sum(a.VT)>=300
ORDER BY 2) b
INNER JOIN
(SELECT 
deliveryid,
avg(timems) as vt_avg
FROM
inscreen_shao.currentposition
WHERE
adid in(35772,35874)
GROUP BY
deliveryId) d
ON b.deliveryid=d.deliveryid 
 ) c
GROUP BY 1
ORDER BY 1;


-- not needed for reports
-- percentile distribution on median view time per viewable impression
SELECT c.percentile*10 as percentile, max(c.vt_med) as ViewTimeMax, 
c.percentile*10-10 as percentile, min(c.vt_med) as ViewTimeMin
FROM
(SELECT d.vt_med,
ntile(10) OVER (order by d.vt_med) as percentile
FROM
(SELECT 
a.deliveryId,
sum(a.VT) as VT
FROM
(SELECT 
deliveryId, -- refreqlogid
sizepct, -- pctInscreen
sum(timems) as VT --incrementalTimeMs
FROM 
inscreen_shao.currentposition
WHERE 
adid in(35772,35874)
GROUP BY
deliveryId,
sizepct) a
WHERE a.sizepct>=30
GROUP BY a.deliveryId HAVING sum(a.VT)>=300
ORDER BY 2) b
INNER JOIN
(SELECT 
distinct deliveryid,
median(timems) over (partition by deliveryid) as vt_med
FROM
inscreen_shao.currentposition
WHERE
adid in(35772,35874)
) d
ON b.deliveryid=d.deliveryid 
 ) c
GROUP BY 1
ORDER BY 1;




