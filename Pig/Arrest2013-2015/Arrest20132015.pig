otable = LOAD '/FinalProject/PartiArrest/part-r-00001' USING PigStorage(';') AS (ReportID:long,ArrestDate:chararray,Year:chararray,Month:chararray,AreaID:long,ReportingDistrict:long,Age:int,SexCode:chararray,DescentCode:chararray,ChargeGroupCode:chararray,ArrestTypeCode:chararray,Charge:chararray,Address:chararray,Location:chararray);
--------------------------------------------------------------------------------------------
/*sorted area by the incidence of Arrest in 2016-2019*/
/*Count Arrest number in each areas 2016-2019.*/
groupArea = GROUP otable BY (AreaID);
count = FOREACH groupArea GENERATE group, COUNT(otable) AS sum;

/*left join with dataset that has areas' detail*/
areadetail = LOAD '/FinalProject/DistinctAreaArrest/part-r-00000' AS (AreaID:long,AreaName:chararray);
joindata1 = JOIN count BY $0 LEFT OUTER, areadetail BY $0;
joindata = FOREACH joindata1 GENERATE $0,$1,$3;

/*sorted by number*/
sorted = ORDER joindata BY sum DESC;

STORE sorted INTO '/FinalProject/PigOut/SortedArrestArea1315';
--------------------------------------------------------------------------------------------
/*sorted Arrest type */
groupType = GROUP otable BY (ArrestTypeCode);
typeCount = FOREACH groupType GENERATE group, COUNT(otable) AS sum;

sortedType = ORDER typeCount BY sum DESC;
STORE sortedType INTO '/FinalProject/PigOut/SortedArrestType1315';
--------------------------------------------------------------------------------------------
/*proportion of 2 genders being Arrested*/
groupGender = Group otable BY (SexCode);
genderCount = FOREACH groupGender GENERATE group, COUNT(otable) AS sum;
temp = GROUP genderCount ALL;
gendersum = FOREACH temp GENERATE SUM(genderCount.sum) AS total;
temp2 = FOREACH genderCount GENERATE $0, $1,ROUND_TO((sum/(double)gendersum.total)*100,2) AS perc;
result = FOREACH temp2 GENERATE $0, $1,CONCAT((chararray)perc,'%');

STORE result INTO '/FinalProject/PigOut/ArrestGenderProportion1315';
--------------------------------------------------------------------------------------------
/*month ratio of Arrest*/
groupMonth = Group otable BY (Month);
monthCount = FOREACH groupMonth GENERATE group, COUNT(otable) AS sum;
monthtemp = GROUP monthCount ALL;
monthsum = FOREACH monthtemp GENERATE SUM(monthCount.sum) AS total;
monthtemp2 = FOREACH monthCount GENERATE $0,$1,ROUND_TO((sum/(double)monthsum.total)*100,2) AS perc;

result = FOREACH monthtemp2 GENERATE $0,$1,CONCAT((chararray)perc,'%');

STORE result INTO '/FinalProject/PigOut/ArrestMonthProportion1315';
--------------------------------------------------------------------------------------------
/*Age ratio of being Arrest*/
groupAge = GROUP otable BY (Age);
ageCount = FOREACH groupAge GENERATE group, COUNT(otable) AS sum;
agetemp = GROUP ageCount ALL;
agesum = FOREACH agetemp GENERATE SUM(ageCount.sum) AS total;
agetemp2 = FOREACH ageCount GENERATE $0,$1,ROUND_TO((sum/(double)agesum.total)*100,4) AS perc;

result = FOREACH agetemp2 GENERATE $0,$1,CONCAT((chararray)perc,'%');

STORE result INTO '/FinalProject/PigOut/ArrestAgeProportion1315';

