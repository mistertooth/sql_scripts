-- create Impact Metrics table
CREATE EXTERNAL TABLE IF NOT EXISTS UserAdImpactMetrics(
timestamp BIGINT,
uid BIGINT,
adid INT,
swipeCount INT,
clickCount INT,
closeCount INT,
totalViewTime BIGINT,
relativeViewTime BIGINT,
adRelevance DOUBLE,
adRecall DOUBLE
)
PARTITIONED BY (date STRING)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '\t'
STORED AS TEXTFILE
LOCATION 's3://wsprofiling/daily/ad-impact-model/user-ad-impact-metrics/';

-- recover partition
MSCK REPAIR TABLE UserAdImpactMetrics;

-- create subset table
CREATE EXTERNAL TABLE IF NOT EXISTS adInterest20151203(
timestamp BIGINT,
uid BIGINT,
adid INT,
swipeCount INT,
clickCount INT,
closeCount INT,
totalViewTime BIGINT,
relativeViewTime BIGINT,
adRelevance DOUBLE,
adRecall DOUBLE
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '\t'
STORED AS TEXTFILE
LOCATION 's3://ws-storage1/etl/adInterest20151203/';
​
-- insert subset data
INSERT OVERWRITE TABLE adInterest20151203
SELECT m2.timestamp, m2.uid, m2.adid, m2.swipecount, m2.clickcount, 
m2.closecount, m2.totalViewTime, m2.relativeViewtime, m2.adRelevance, 
m2.adRecall
FROM
(select adid, uid, max(timestamp) as timestamp
from UserAdImpactMetrics
where date>='2015-11-13'
group by adid, uid) m1
INNER JOIN
UserAdImpactMetrics m2
ON  m1.adid=m2.adid
AND m1.uid=m2.uid
AND m1.timestamp=m2.timestamp
WHERE m2.date>='2015-11-13';


-- create table subset requestlog based on device info
CREATE EXTERNAL TABLE IF NOT EXISTS requestlog_device_subset20151203(
deviceModelId bigint,
adid int,
uid bigint,
validRequests int,
deliveredImp int,
monetizedImp int
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '\t'
STORED AS TEXTFILE
LOCATION 's3://ws-storage1/etl/requestlog_device_subset20151203/';

-- insert subset of requestlog
INSERT OVERWRITE TABLE requestlog_device_subset20151203
SELECT 
opti13,
adid,
uid,
count(distinct case when optb5=1 and valid=1 then requestlogid else null end),
count(distinct case when valid=1 and optb34=0 then requestlogid else null end),
count(distinct case when validimp=1	and adid<>22 then requestlogid else null end)
FROM requestlog
WHERE created1>='2015-11-13'
and created1<='2015-12-03'
GROUP by 
deviceModelId,
adid,
uid;

-- push device dim data to s3
s3cmd put /Users/mistertooth/Desktop/whatever/deviceModel.txt s3://ws-storage1/etl/devicemodel/

-- create hive table on device dim data
CREATE EXTERNAL TABLE IF NOT EXISTS devicemodel(
deviceModelId bigint,
modelRegDate string,
modelName string,
deviceManufacturerId int,
manufacturerName string,
modelMarketingName string,
deviceType string,
platformId int,
platformName string,
screenWidth int,
screenSize int
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '\t'
STORED AS TEXTFILE
LOCATION 's3://ws-storage1/etl/devicemodel/';


-- join req, interest and dim data together
CREATE EXTERNAL TABLE IF NOT EXISTS device_req_interest20151204(
adid int,
uid int,
deviceModelId int,
modelregdate string,
modelname string,
deviceManufacturerId int,
manufacturerName string,
modelMarketingName string,
deviceType string,
platformId int,
platformName string,
screenWidth int,
screenSize double,
swipeCount INT,
clickCount INT,
closeCount INT,
totalViewTime BIGINT,
relativeViewTime BIGINT,
adRelevance DOUBLE,
adRecall DOUBLE,
validRequests int,
deliveredImp int,
monetizedImp int
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '\t'
STORED AS TEXTFILE
LOCATION 's3://ws-storage1/etl/device_req_interest20151204/';

INSERT OVERWRITE TABLE device_req_interest20151204
SELECT 
r.adid ,
r.uid ,
d.deviceModelId ,
d.modelregdate ,
d.modelname ,
d.deviceManufacturerId ,
d.manufacturerName ,
d.modelMarketingName ,
d.deviceType ,
d.platformId ,
d.platformName ,
d.screenWidth ,
d.screenSize ,
i.swipeCount ,
i.clickCount ,
i.closeCount ,
i.totalViewTime ,
i.relativeViewTime ,
i.adRelevance ,
i.adRecall ,
r.validRequests ,
r.deliveredImp ,
r.monetizedImp 
FROM 
devicemodel d
INNER JOIN
requestlog_device_subset20151203 r 
ON d.devicemodelid=r.deviceModelId
INNER JOIN
adInterest20151203 i 
ON i.adid=r.adid and i.uid=r.uid;