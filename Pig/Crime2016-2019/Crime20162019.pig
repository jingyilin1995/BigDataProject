otable = LOAD '/FinalProject/PartiCrime/part-r-00002' USING PigStorage(';') AS (DRNumber:long,DateReported:chararray,Year:chararray,Month:chararray,DateOccurred:chararray,TimeOccurred:long,
AreaID:long,ReportingDistrict:long,CrimeCode:long,VictimAge:int,VictimSex:chararray,
VictimDescent:chararray,PremiseCode:long,WeaponUsed:long,StatusCode:chararray,CrimeCode2:long,Address:chararray,Location:chararray);
--------------------------------------------------------------------------------------------
/*sorted area by the incidence of Arrest in 2016-2019*/
/*Count Arrest number in each areas 2016-2019.*/
groupArea = GROUP otable BY (AreaID);
count = FOREACH groupArea GENERATE group, COUNT(otable) AS sum;

/*left join with dataset that has areas' detail*/
areadetail = LOAD '/FinalProject/DistinctAreaCrime/part-r-00000' AS (AreaID:long,AreaName:chararray);
joindata1 = JOIN count BY $0 LEFT OUTER, areadetail BY $0;
joindata = FOREACH joindata1 GENERATE $0,$1,$3;

/*sorted by number*/
sorted = ORDER joindata BY sum DESC;

STORE sorted INTO '/FinalProject/PigOut/SortedCrimeArea';
--------------------------------------------------------------------------------------------
/*Top 10 crime Types occur most grequently each year*/
crimeCodeDetail = LOAD '/FinalProject/DistinctCrime/part-r-00000' AS (CrimeCode:long,Describe:chararray);
groupCrimeCode = GROUP otable BY (Year,CrimeCode);
crimeCount = FOREACH groupCrimeCode GENERATE group.Year,group.CrimeCode, COUNT(otable) AS sum;

joindata1 = JOIN crimeCount BY $1 LEFT OUTER, crimeCodeDetail BY $0;
joinresult = FOREACH joindata1 GENERATE $0,$1,$2,$4;

groupCountCrime = GROUP joinresult BY $0;
resultCrimeCode = FOREACH groupCountCrime {
	sorted = ORDER joinresult BY $2 DESC;
	lim = LIMIT sorted 10;
	GENERATE FLATTEN(lim);
}

STORE resultCrimeCode INTO '/FinalProject/PigOut/SortedCrimeCode';
--------------------------------------------------------------------------------------------
/*month ratio of Crime*/
groupMonth = Group otable BY (Month);
monthCount = FOREACH groupMonth GENERATE group, COUNT(otable) AS sum;
monthtemp = GROUP monthCount ALL;
monthsum = FOREACH monthtemp GENERATE SUM(monthCount.sum) AS total;
monthtemp2 = FOREACH monthCount GENERATE $0,$1,ROUND_TO((sum/(double)monthsum.total)*100,2) AS perc;

result = FOREACH monthtemp2 GENERATE $0,$1,CONCAT((chararray)perc,'%');

STORE result INTO '/FinalProject/PigOut/CrimeMonthProportion';
--------------------------------------------------------------------------------------------
/*Victim Age ratio*/
groupVicAge = Group otable BY (VictimAge);
ageCount = FOREACH groupVicAge GENERATE group, COUNT(otable) AS sum;
agetemp = GROUP ageCount ALL;
agesum = FOREACH agetemp GENERATE SUM(ageCount.sum) AS total;
agetemp2 = FOREACH ageCount GENERATE $0,$1,ROUND_TO((sum/(double)agesum.total)*100,4) AS perc;

result = FOREACH agetemp2 GENERATE $0,$1,CONCAT((chararray)perc,'%');

STORE result INTO '/FinalProject/PigOut/CrimeVicAgeProportion';
--------------------------------------------------------------------------------------------
/*Proportion of victim genders */
groupGender = Group otable BY (VictimSex);
genderCount = FOREACH groupGender GENERATE group, COUNT(otable) AS sum;
temp = GROUP genderCount ALL;
gendersum = FOREACH temp GENERATE SUM(genderCount.sum) AS total;
temp2 = FOREACH genderCount GENERATE $0, $1,ROUND_TO((sum/(double)gendersum.total)*100,2) AS perc;
result = FOREACH temp2 GENERATE $0, $1,CONCAT((chararray)perc,'%');

STORE result INTO '/FinalProject/PigOut/CrimeVicGenderProportion';

--------------------------------------------------------------------------------------------
/*Proportion and detail of Weapons used of crime*/
weapondetail = LOAD '/FinalProject/DistinctWeaponUsed/part-r-00000' AS (WeaponUsed:long,WeaponDescribed:chararray);
groupWeapon = Group otable BY (WeaponUsed);
WeaponCount = FOREACH groupWeapon GENERATE group, COUNT(otable) AS sum;
Weapontemp = GROUP WeaponCount ALL;
Weaponsum = FOREACH Weapontemp GENERATE SUM(WeaponCount.sum) AS total;
Weapontemp2 = FOREACH WeaponCount GENERATE $0,$1,ROUND_TO((sum/(double)Weaponsum.total)*100,4) AS perc;
result = FOREACH Weapontemp2 GENERATE $0,$1,CONCAT((chararray)perc,'%');

joindata1 = JOIN result BY $0 LEFT OUTER, weapondetail BY $0;
joindata = FOREACH joindata1 GENERATE $0,$1,$2,$4;

sorted = ORDER joindata BY $1 DESC;
STORE sorted INTO '/FinalProject/PigOut/CrimeWeaponProportion';

--------------------------------------------------------------------------------------------
/*Different Status proportion of Crime*/
statusdetail = LOAD '/FinalProject/DistinctStatus/part-r-00000' AS (StatusCode:chararray,StatusDescribed:chararray);
groupStatus = Group otable BY (StatusCode);
StatusCount = FOREACH groupStatus GENERATE group, COUNT(otable) AS sum;
Statustemp = GROUP StatusCount ALL;
Statussum = FOREACH Statustemp GENERATE SUM(StatusCount.sum) AS total;
Statustemp2 = FOREACH StatusCount GENERATE $0,$1,ROUND_TO((sum/(double)Statussum.total)*100,4) AS perc;
result = FOREACH Statustemp2 GENERATE $0,$1,CONCAT((chararray)perc,'%');

joindata1 = JOIN result BY $0 LEFT OUTER, statusdetail BY $0;
joindata = FOREACH joindata1 GENERATE $0,$1,$2,$4;

sorted = ORDER joindata BY $1 DESC;
STORE sorted INTO '/FinalProject/PigOut/CrimeStatusProportion';
--------------------------------------------------------------------------------------------
/*Descent*/
groupDescent = Group otable BY (VictimDescent);
descentCount = FOREACH groupDescent GENERATE group, COUNT(otable) AS sum;
temp = GROUP descentCount ALL;
descentsum = FOREACH temp GENERATE SUM(descentCount.sum) AS total;
temp2 = FOREACH descentCount GENERATE $0, $1,ROUND_TO((sum/(double)descentsum.total)*100,2) AS perc;
result = FOREACH temp2 GENERATE $0, $1,CONCAT((chararray)perc,'%');

STORE result INTO '/FinalProject/PigOut/CrimeVicDescentProportion';


