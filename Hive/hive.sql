CREATE TABLE arrest1619 (ReportID INT,ArrestDate STRING,Year STRING,Month STRING,
AreaID INT,ReportingDistrict INT,Age INT,SexCode STRING,DescentCode STRING,
ChargeGroupCode STRING,ArrestTypeCode STRING,Charge STRING,Address STRING,Location STRING)
ROW FORMAT DELIMITED FIELDS TERMINATED BY ";"
STORED AS TEXTFILE;

LOAD DATA INPATH "/FinalProject/PartiArrest/part-r-00002" INTO TABLE arrest1619;

CREATE TABLE arrest1315 (ReportID INT,ArrestDate STRING,Year STRING,Month STRING,AreaID INT,ReportingDistrict INT,Age INT,SexCode STRING,DescentCode STRING,ChargeGroupCode STRING,ArrestTypeCode STRING,Charge STRING,Address STRING,Location STRING)
ROW FORMAT DELIMITED FIELDS TERMINATED BY ";"
STORED AS TEXTFILE;

LOAD DATA INPATH "/FinalProject/PartiArrest/part-r-00001" INTO TABLE arrest1315;

CREATE TABLE crime1619 (DRNumber INT,DateReported STRING,Year STRING,Month STRING,DateOccurred STRING,TimeOccurred INT,AreaID INT,ReportingDistrict INT,CrimeCode INT,VictimAge INT,VictimSex STRING,VictimDescent STRING,PremiseCode INT,WeaponUsed INT,StatusCode STRING,CrimeCode2 INT,Address STRING,Location STRING)
ROW FORMAT DELIMITED FIELDS TERMINATED BY ";"
STORED AS TEXTFILE;

LOAD DATA INPATH "/FinalProject/PartiCrime/part-r-00002" INTO TABLE crime1619;

CREATE TABLE crime1315 (DRNumber INT,DateReported STRING,Year STRING,Month STRING,DateOccurred STRING,TimeOccurred INT,AreaID INT,ReportingDistrict INT,CrimeCode INT,VictimAge INT,VictimSex STRING,VictimDescent STRING,PremiseCode INT,WeaponUsed INT,StatusCode STRING,CrimeCode2 INT,Address STRING,Location STRING)
ROW FORMAT DELIMITED FIELDS TERMINATED BY ";"
STORED AS TEXTFILE;

LOAD DATA INPATH "/FinalProject/PartiCrime/part-r-00001" INTO TABLE crime1315;

CREATE TABLE sortArrestArea (AreaID INT,AreaCount INT,Name STRING)
PARTITIONED BY (tl STRING)
ROW FORMAT DELIMITED FIELDS TERMINATED BY "\t";

LOAD DATA INPATH "/FinalProject/PigOut/SortedArrestArea/part-r-00000" INTO TABLE sortArrestArea PARTITION (tl="1619");
LOAD DATA INPATH "/FinalProject/PigOut/SortedArrestArea1315/part-r-00000" INTO TABLE sortArrestArea PARTITION (tl="1315");

CREATE TABLE sortArrestType (TypeID STRING,TypeCount INT)
PARTITIONED BY (tl STRING)
ROW FORMAT DELIMITED FIELDS TERMINATED BY "\t";

LOAD DATA INPATH "/FinalProject/PigOut/SortedArrestType/part-r-00000" INTO TABLE sortArrestType PARTITION (tl="1619");
LOAD DATA INPATH "/FinalProject/PigOut/SortedArrestType1315/part-r-00000" INTO TABLE sortArrestType PARTITION (tl="1315");

CREATE TABLE ArrestMonthProportion (Month STRING,MonthCount INT,Perc STRING)
PARTITIONED BY (tl STRING)
ROW FORMAT DELIMITED FIELDS TERMINATED BY "\t";

LOAD DATA INPATH "/FinalProject/PigOut/ArrestMonthProportion/part-m-00000" INTO TABLE ArrestMonthProportion PARTITION (tl="1619");
LOAD DATA INPATH "/FinalProject/PigOut/ArrestMonthProportion1315/part-m-00000" INTO TABLE ArrestMonthProportion PARTITION (tl="1315");

CREATE TABLE ArrestGenderProportion (Gender STRING,GenderCount INT,Perc STRING)
PARTITIONED BY (tl STRING)
ROW FORMAT DELIMITED FIELDS TERMINATED BY "\t";

LOAD DATA INPATH "/FinalProject/PigOut/ArrestGenderProportion/part-m-00000" INTO TABLE ArrestGenderProportion PARTITION (tl="1619");
LOAD DATA INPATH "/FinalProject/PigOut/ArrestGenderProportion1315/part-m-00000" INTO TABLE ArrestGenderProportion PARTITION (tl="1315");

CREATE TABLE ArrestAgeProportion (Age INT,AgeCount INT,Perc STRING)
PARTITIONED BY (tl STRING)
ROW FORMAT DELIMITED FIELDS TERMINATED BY "\t";

LOAD DATA INPATH "/FinalProject/PigOut/ArrestAgeProportion/part-m-00000" INTO TABLE ArrestAgeProportion PARTITION (tl="1619");
LOAD DATA INPATH "/FinalProject/PigOut/ArrestAgeProportion1315/part-m-00000" INTO TABLE ArrestAgeProportion PARTITION (tl="1315");

CREATE TABLE SortCrimeCode (Year String,CrimeCode INT,CrimeCount INT,Detail STRING)
PARTITIONED BY (tl STRING)
ROW FORMAT DELIMITED FIELDS TERMINATED BY "\t";

LOAD DATA INPATH "/FinalProject/PigOut/SortedCrimeCode/part-r-00000" INTO TABLE SortCrimeCode PARTITION (tl="1619");
LOAD DATA INPATH "/FinalProject/PigOut/SortedCrimeCode1315/part-r-00000" INTO TABLE SortCrimeCode PARTITION (tl="1315");

CREATE TABLE SortCrimeArea (AreaID INT,AreaCount INT,Name STRING)
PARTITIONED BY (tl STRING)
ROW FORMAT DELIMITED FIELDS TERMINATED BY "\t";

LOAD DATA INPATH "/FinalProject/PigOut/SortedCrimeArea/part-r-00000" INTO TABLE SortCrimeArea PARTITION (tl="1619");
LOAD DATA INPATH "/FinalProject/PigOut/SortedCrimeArea1315/part-r-00000" INTO TABLE SortCrimeArea PARTITION (tl="1315");

CREATE TABLE CrimeWeaponProportion (WeaponCode INT,WeaponCount INT,Perc STRING,detail STRING)
PARTITIONED BY (tl STRING)
ROW FORMAT DELIMITED FIELDS TERMINATED BY "\t";

LOAD DATA INPATH "/FinalProject/PigOut/CrimeWeaponProportion/part-r-00000" INTO TABLE CrimeWeaponProportion PARTITION (tl="1619");
LOAD DATA INPATH "/FinalProject/PigOut/CrimeWeaponProportion1315/part-r-00000" INTO TABLE CrimeWeaponProportion PARTITION (tl="1315");

CREATE TABLE CrimeVicGenderProportion (Gender STRING,GenderCount INT,Perc STRING)
PARTITIONED BY (tl STRING)
ROW FORMAT DELIMITED FIELDS TERMINATED BY "\t";

LOAD DATA INPATH "/FinalProject/PigOut/CrimeVicGenderProportion/part-m-00000" INTO TABLE CrimeVicGenderProportion PARTITION (tl="1619");
LOAD DATA INPATH "/FinalProject/PigOut/CrimeVicGenderProportion1315/part-m-00000" INTO TABLE CrimeVicGenderProportion PARTITION (tl="1315");

CREATE TABLE CrimeVicDescentProportion (Descent STRING,DescentCount INT,Perc STRING)
PARTITIONED BY (tl STRING)
ROW FORMAT DELIMITED FIELDS TERMINATED BY "\t";

LOAD DATA INPATH "/FinalProject/PigOut/CrimeVicDescentProportion/part-m-00000" INTO TABLE CrimeVicDescentProportion PARTITION (tl="1619");
LOAD DATA INPATH "/FinalProject/PigOut/CrimeVicDescentProportion1315/part-m-00000" INTO TABLE CrimeVicDescentProportion PARTITION (tl="1315");

CREATE TABLE CrimeVicAgeProportion (Age INT,AgeCount INT,Perc STRING)
PARTITIONED BY (tl STRING)
ROW FORMAT DELIMITED FIELDS TERMINATED BY "\t";

LOAD DATA INPATH "/FinalProject/PigOut/CrimeVicAgeProportion/part-m-00000" INTO TABLE CrimeVicAgeProportion PARTITION (tl="1619");
LOAD DATA INPATH "/FinalProject/PigOut/CrimeVicAgeProportion1315/part-m-00000" INTO TABLE CrimeVicAgeProportion PARTITION (tl="1315");

CREATE TABLE CrimeStatusProportion (Status STRING,StatusCount INT,detail STRING)
PARTITIONED BY (tl STRING)
ROW FORMAT DELIMITED FIELDS TERMINATED BY "\t";

LOAD DATA INPATH "/FinalProject/PigOut/CrimeStatusProportion/part-r-00000" INTO TABLE CrimeStatusProportion PARTITION (tl="1619");
LOAD DATA INPATH "/FinalProject/PigOut/CrimeStatusProportion1315/part-r-00000" INTO TABLE CrimeStatusProportion PARTITION (tl="1315");

CREATE TABLE CrimeMonthProportion (Month INT,MonthCount INT,Perc STRING)
PARTITIONED BY (tl STRING)
ROW FORMAT DELIMITED FIELDS TERMINATED BY "\t";

LOAD DATA INPATH "/FinalProject/PigOut/CrimeMonthProportion/part-m-00000" INTO TABLE CrimeMonthProportion PARTITION (tl="1619");
LOAD DATA INPATH "/FinalProject/PigOut/CrimeMonthProportion1315/part-m-00000" INTO TABLE CrimeMonthProportion PARTITION (tl="1315");

/*-------------------------------------------------------------------QUERY----------------------------------------------------------*/
SELECT * FROM CrimeVicAgeProportion WHERE Age <= 14;
SELECT * FROM ArrestAgeProportion WHERE Age <= 14 AND tl="1619"; 
SELECT * FROM sortArrestType WHERE tl="1619"; 
SELECT Age, SexCode,DescentCode,ArrestTypeCode FROM arrest1619 WHERE Age == 0 AND tl = "1619"; 




