


-- Query survey result and save result on dwh server
mysql -h 127.0.0.1 -u xxxxx -P xxxx survey_new -p'xxxxx' -e  '
select 
sas.surveyid, 
a.adid as originalAdId, 
sas.adId as surveyAdId,
a.templatetype as format,
substring(a.adname, 2,2) as market, 
a.adstarttime as startTime, 
a.adEndtime as endTime,
sas.created_at as surveyResponceTime,
sas.uid as uid,
sas.answerSetId as answerSetId,
sa.answerId as answerId,
sa.questionId as questionId,
sq_l.text as questionText,
sa.optionId as optionId,
sol.text as optionText,
sas.iscomplete as iscomplete,
sas.isRefGroup as isRefGroup
from survey_new.SurveyAnswerSets sas 
JOIN survey_new.SurveyAnswers sa
on sa.answersetid=sas.answersetid
join survey_new.Surveys_OriginalAds so
on sas.surveyid=so.surveyid 
join online.dimAd a 
on so.originaladid=a.adid
JOIN survey_new.SurveyOptions_Languages sol on sa.optionId=sol.optionId
JOIN survey_new.SurveyQuestions_Languages sq_l on sq_l.questionid=sa.questionId
where sol.languageId="en-GB"
and sq_l.languageid="en-GB";
'  > /tmp/bis_20151012.txt

-- ssh to file and download to local default folder, then remove header text

-- push to s3
s3cmd put bis_20151012.txt  s3://ws-storage1/etl/bis_20151012/

scp -i ~/Desktop/xxxxx.pem ec2-user@xxxxxx.compute.amazonaws.com:/tmp/bis_20151012.txt .

-- hive query join survey result and requestlog

nohup hive -e "CREATE EXTERNAL TABLE IF NOT EXISTS bis_20151012_traffic 
(
adSpaceId int,
premiumLevel int,
surveyid int,
originalAdId int,
surveyAdId int,
format string, 
market string,
uid bigint,
answersetId int,
questionId int,
questionText string,
optionId int,
optionText string,
deliveries int,
ViewableImps int
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '\t'
STORED AS TEXTFILE
LOCATION 's3://ws-storage1/etl/bis_20151012_traffic/';


INSERT OVERWRITE TABLE bis_20151012_traffic
SELECT
-- from requestlog table
r.adSpaceid ,
r.optb31 as premiumLevel ,
-- from survey tables
bis.surveyid,
bis.originalAdId ,
bis.surveyAdId ,
bis.format , 
bis.market ,
bis.uid ,
bis.answersetId ,
bis.questionId ,
bis.questionText ,
bis.optionId ,
bis.optionText ,
count(case when optb5=1 and valid=1 and adid<>22 then requestlogid else null end) as Deliveries,
count(case when optb34=1 and valid=1 then requestlogid else null end) as ViewableImps
FROM
bis_20151012 bis
JOIN
requestlog r
ON
bis.uid=r.uid
AND
bis.originalAdId=r.adid
WHERE
r.created1>=to_date(bis.starttime)
AND
r.created1<=to_date(bis.endtime)
AND
r.created<bis.surveyResponceTime
AND
bis.iscomplete=1
AND
to_date(bis.starttime)>='2015-06-15'
AND
r.created1>='2015-06-15'
AND
bis.surveyid in (11,12,14,15,16,17,18,20,21,41,87,92,95,98,99,100,104,105,106,107,108,115,116,118,122,130,134,136,156,157,158,159,160)
GROUP BY
r.adSpaceid ,
r.optb31 ,
bis.surveyid,
bis.originalAdId ,
bis.surveyAdId ,
bis.format , 
bis.market ,
bis.uid ,
bis.answersetId ,
bis.questionId ,
bis.questionText ,
bis.optionId ,
bis.optionText ;"
