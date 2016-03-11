select 
sas.surveyid, 
a.adid as originalAdId, 
sas.adId as surveyAdId,
a.templatetype as format,
substring(a.name, 2,2) as market, 
a.starttime as startTime, 
a.endtime as endTime,
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
from public.surveydb_SurveyAnswerSets sas 
JOIN public.surveydb_SurveyAnswers sa
on sa.answersetid=sas.answersetid
join public.surveydb_Surveys_OriginalAds so
on sas.surveyid=so.surveyid 
join public.novadb_online_ad a 
on so.originaladid=a.adid
JOIN public.surveydb_SurveyOptions_Languages sol on sa.optionId=sol.optionId
JOIN public.surveydb_SurveyQuestions_Languages sq_l on sq_l.questionid=sa.questionId
where sol.languageId='en-GB'
and sq_l.languageid='en-GB'
and sas.surveyid in (457);