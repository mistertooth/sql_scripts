
-- OAS001460 Sunniva
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
adid in (37569,37880)
GROUP BY
deliveryId,
sizepct) a
WHERE a.sizepct>=30
GROUP BY a.deliveryId HAVING sum(a.VT)>=300
ORDER BY 2) b ) c
GROUP BY 1
ORDER BY 1;



-- percentile distribution by adspace
SELECT c.apptype, c.adspaceid, c.name, c.percentile*10 as percentile, max(c.vt) as ViewTimeMax
FROM
(SELECT b.apptype,b.adspaceid, b.name, b.VT, 
ntile(10) OVER (order by b.VT) as percentile
FROM
(SELECT 
a.apptype,
a.deliveryId,
a.adspaceid,
a.name,
sum(a.VT) as VT
FROM
(SELECT 
s.apptype,
s.adspaceid,
s.name,
deliveryId, -- refreqlogid
sizepct, -- pctInscreen
sum(timems) as VT --incrementalTimeMs
FROM 
inscreen_shao.currentposition i
INNER JOIN
public.novadb_online_adspace s 
ON i.sid=s.extsiteid
WHERE 
adid in (37569,37880)
GROUP BY
s.apptype,
s.adspaceid,
s.name,
deliveryId,
sizepct) a
WHERE a.sizepct>=30
GROUP BY a.apptype, a.adspaceid, a.name, a.deliveryId HAVING sum(a.VT)>=300
ORDER BY 5) b ) c
GROUP BY 1,2,3,4
ORDER BY 1,2;





-- average view time by adspace and count of viewable imp
SELECT 
b.apptype,
b.adspaceid, 
b.name as adSpace, 
avg(b.VT) as avgViewTime, 
max(b.VT) as maxViewtime, 
min(b.VT) as minViewTime, 
count(distinct b.deliveryid) as numImpression
FROM
(SELECT 
a.apptype,
a.deliveryId,
a.adspaceid,
a.name,
sum(a.VT) as VT
FROM
(SELECT 
s.apptype,
s.adspaceid,
s.name,
deliveryId, -- refreqlogid
sizepct, -- pctInscreen
sum(timems) as VT --incrementalTimeMs
FROM 
inscreen_shao.currentposition i
INNER JOIN
public.novadb_online_adspace s 
ON i.sid=s.extsiteid
WHERE 
adid in (37569,37880)
GROUP BY
s.apptype,
s.adspaceid,
s.name,
deliveryId,
sizepct) a
WHERE a.sizepct>=30
GROUP BY a.apptype, a.adspaceid, a.name, a.deliveryId HAVING sum(a.VT)>=300) b 
GROUP BY 1,2,3
ORDER BY 1,2,3
;

-- 180.no investigation on the biggest outlier
-- top 10 deliveries with the hightest view time
SELECT 
a.deliveryId,
sum(a.VT) as VT
FROM
(SELECT 
s.apptype,
s.adspaceid,
s.name,
deliveryId, -- refreqlogid
sizepct, -- pctInscreen
sum(timems) as VT --incrementalTimeMs
FROM 
inscreen_shao.currentposition i
INNER JOIN
public.novadb_online_adspace s 
ON i.sid=s.extsiteid
WHERE 
i.adid in (37569,37880)
AND
s.adspaceid = 13369
GROUP BY
s.apptype,
s.adspaceid,
s.name,
deliveryId,
sizepct) a
WHERE a.sizepct>=30
GROUP BY 1 HAVING sum(a.VT)>=300
ORDER BY 2 DESC 
LIMIT 10
;

-- 180.no
-- inscreen info on the problematic deliveries
SELECT * FROM inscreen_shao.currentposition
WHERE deliveryid in (
89877144892,
89881138509,
89887196740,
89876677082,
89878163547,
91471137934,
91015448955,
90917310279,
89993593680,
91122874928)
ORDER BY deliveryId, timestamp;



-- 180.no
-- RL2 information on the problematic deliveries
SELECT
d.deliveryid,
d.clientIp,
d.user_uid,
d.device_devicemodelId,
m.modelname,
m.marketingname,
d.device_browserId,
b.name as browserName,
d.client_clientversionid,
v.sdkclientname as sdkClientName,
v.version as sdkClientVersion
FROM public.rl2_delivery d 
INNER JOIN public.novadb_online_browser b
ON d.device_browserId = b.browserid
INNER JOIN public.novadb_online_sdkclientversion v
ON d.client_clientversionid=v.sdkclientversionid
INNER JOIN public.novadb_online_devicemodel m
ON d.device_devicemodelId=m.devicemodelid
WHERE ad_adid in (37569,37880)
AND d.deliveryid in (
89877144892,
89881138509,
89887196740,
89876677082,
89878163547,
91471137934,
91015448955,
90917310279,
89993593680,
91122874928)
AND d.timestamp >= '2015-11-29 00:00:00'
AND d.timestamp < '2015-12-21 00:00:00';


-- sunniva
-- top 10 deliveries with highest view time from each apptype Nov
SELECT
c.apptype,
c.deliveryid,
c.adspaceid,
c.name,
c.vt 
FROM
(SELECT 
*,
ROW_NUMBER() OVER (PARTITION BY b.apptype ORDER BY b.VT DESC) as row_id
FROM
(SELECT 
a.apptype,
a.deliveryId,
a.adspaceid,
a.name,
sum(a.VT) as VT
FROM
(SELECT 
s.apptype,
s.adspaceid,
s.name,
deliveryId, -- refreqlogid
sizepct, -- pctInscreen
sum(timems) as VT --incrementalTimeMs
FROM 
inscreen_shao.currentposition i
INNER JOIN
public.novadb_online_adspace s 
ON i.sid=s.extsiteid
WHERE 
adid in (37569,37880)
GROUP BY
s.apptype,
s.adspaceid,
s.name,
deliveryId,
sizepct) a
WHERE a.sizepct>=30
GROUP BY a.apptype, a.adspaceid, a.name, a.deliveryId HAVING sum(a.VT)>=300) b ) c
WHERE
c.row_id <=10
;

-- sunniva
-- inscreen info on the problematic deliveries on ios app
SELECT * FROM inscreen_shao.currentposition
WHERE deliveryid in (
89977984888
90655233213
91109165118
90679807029
91126199645
89985215182
89982086005
91968798559
89790335597
90558721372)
ORDER BY deliveryId, timestamp;



-- sunniva
-- RL2 information on the problematic deliveries on ios app
SELECT
d.deliveryid,
d.clientIp,
d.user_uid,
d.device_devicemodelId,
m.modelname,
m.marketingname,
d.device_browserId,
b.name as browserName,
d.client_clientversionid,
v.sdkclientname as sdkClientName,
v.version as sdkClientVersion
FROM public.rl2_delivery d 
INNER JOIN public.novadb_online_browser b
ON d.device_browserId = b.browserid
INNER JOIN public.novadb_online_sdkclientversion v
ON d.client_clientversionid=v.sdkclientversionid
INNER JOIN public.novadb_online_devicemodel m
ON d.device_devicemodelId=m.devicemodelid
WHERE ad_adid in (37569,37880)
AND d.deliveryid in (
89977984888,
90655233213,
91109165118,
90679807029,
91126199645,
89985215182,
89982086005,
91968798559,
89790335597,
90558721372)
AND d.timestamp >= '2015-11-29 00:00:00'
AND d.timestamp < '2015-12-21 00:00:00';



