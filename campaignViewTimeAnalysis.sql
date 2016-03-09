
#campaign view time analysis 2

-- find adid in config DB
-- make sure the campaign period is covered in log data in new redshift

select starttime, endtime, adid, name
from Ad
where name like '%OSE002901%';
# 38768


-- 1 OSE002901 HP StarWars Takeover Viewable (CPMV) percentile view time by viewableImp

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
public.inscreen_currentposition
WHERE 
adid in(38768)
GROUP BY
deliveryId,
sizepct) a
WHERE a.sizepct>=30
GROUP BY a.deliveryId HAVING sum(a.VT)>=300
ORDER BY 2) b ) c
GROUP BY 1
ORDER BY 1;


-- 2 OSE002901 HP StarWars Takeover Viewable (CPMV) percentile distribution of view time on measurable impression
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
FULL OUTER JOIN  public.inscreen_currentposition c ON (c.deliveryid=d.deliveryid AND c.adid in (38768))
WHERE d.ad_adid in (38768)
AND  i.valid = 1 AND i.impressiontype = 0 AND (capabilitiesmask & 4294967296) > 0
AND d.timestamp >= '2015-12-19 00:00:00' AND d.timestamp < '2016-01-01 00:00:00' 
GROUP BY
1
ORDER BY 1) b ) c
GROUP BY 1
ORDER BY 1;

-- Calculate median (50th percentile uplift based on results from 1 & 2)
# (4078-1561)/1561 = 161%

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
public.inscreen_currentposition
WHERE 
adid in(38768)
GROUP BY
deliveryId,
sizepct) a
WHERE a.sizepct>=30
GROUP BY a.deliveryId HAVING sum(a.VT)>=300) b 
;

#39084	3426474

--4 avg view time by inscreenImp
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
public.inscreen_currentposition
WHERE 
adid in(38768)
GROUP BY
deliveryId,
sizepct) a
WHERE a.sizepct>0
GROUP BY a.deliveryId ) b ;

# 43684	4051793

-- 5 impression funnel on the campaign
SELECT 
COUNT(DISTINCT CASE WHEN i.valid = 1 AND i.impressiontype = 0 AND (capabilitiesmask & 4294967296) > 0 THEN d.deliveryid END) AS measurableImp_RL2
FROM public.rl2_delivery d 
INNER JOIN public.rl2_impression i ON (d.deliveryid=i.deliveryid) 
WHERE d.ad_adid in (38768)
AND  i.valid = 1
AND d.timestamp >= '2015-12-19 00:00:00' AND d.timestamp < '2016-01-01 00:00:00' 
;

#6490469

-- calculate avg view time uplift from 3 & 4 & 5, this is a hack, for batch process, we can do it more str8 foward

# avgViewTimeMeasurable 43684*4051793/6490469 =27271
# uplift viewable VS measurable (39084-27271)/27271 = 43.32%

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
public.inscreen_currentposition
WHERE 
adid in(38768)
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
public.inscreen_currentposition i
INNER JOIN
public.novadb_online_adspace s 
ON i.sid=s.extsiteid
WHERE 
adid in(38768)
GROUP BY
s.apptype,
deliveryId,
sizepct) a
WHERE a.sizepct>=30
GROUP BY a.apptype,a.deliveryId HAVING sum(a.VT)>=300
ORDER BY 3) b ) c
GROUP BY 1,2
ORDER BY 1,2;



-- OSE002759 Opel Astra Takeover Split Video / Viewable (CPMV)
-- avg size percentage by the max pct per viewable impression
select 
count(distinct b.deliveryid) as viewableImp, 
max(b.sizepct) as maxPct, 
min(b.sizepct) as minPct,
avg(b.sizepct) as avgPct
from
(SELECT a.deliveryid, max(a.sizepct) as sizepct
FROM
(SELECT 
deliveryId, --refreqlogid
sizepct, --inscreenPct
sum(timems) as VT --incrementalTimeMs
FROM 
inscreen_shao.currentposition
WHERE 
adId in (36304,37578)
and 
sizepct>=30
GROUP BY
deliveryId, sizepct having sum(timems)>=300) a
group by a.deliveryid) b;

#viewableimp	maxpct	minpct	avgpct
#3047054	    100	     30	    87

-- OSE002759 Opel Astra Takeover Split Video / Viewable (CPMV)
-- median size percentage by the max pct per viewable impression
SELECT c.percentile*10 as percentile, max(c.sizepct) as pct_HighBound, 
c.percentile*10-10 as percentile, min(c.sizepct) as pct_LowBound
FROM
(SELECT b.sizepct,
ntile(10) OVER (order by b.sizepct) as percentile
FROM
(SELECT a.deliveryid, max(a.sizepct) as sizepct
FROM
(SELECT 
deliveryId, --refreqlogid
sizepct, --inscreenPct
sum(timems) as VT --incrementalTimeMs
FROM 
inscreen_shao.currentposition
WHERE 
adId in (36304,37578)
and 
sizepct>=30
GROUP BY
deliveryId, sizepct having sum(timems)>=300) a
group by a.deliveryid) b
) c
GROUP BY 1
ORDER BY 1;
;
