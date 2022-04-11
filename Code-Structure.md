<h3>Importing Customer_individual view From Mysql Adventureworks Database to HDFS Using Sqoop</h3>

sqoop import --connect jdbc:mysql://localhost:3306/adventureworks?useSSL=False \\<br>
--username root --password-file file:///home/saif/LFS/cohort_c9/envvar/sqoop.pwd \\<br>
--delete-target-dir --target-dir /user/saif/HFS/Input/adventureworks \\<br>
--query "select CustomerID,TerritoryID,AccountNumber,CustomerType,ModifiedDate,Demographics from customer_individual \\<br>
where \$CONDITIONS" --split-by CustomerID


<h3>Creation Of Hive manage table & Load data from hdfs in manage table</h3>
create table cust_ind<br>
(<br>
CustomerID int,<br>
TerritoryID int,<br>
AccountNumber string,<br>
CustomerType string,<br>
ModifiedDate timestamp,<br>
Demographics string<br>
) <br>
row format delimited fields terminated by ','<br> 
tblproperties("skip.header.line.count"="1") ;<br>

load data inpath "/user/saif/HFS/Input/adventureworks/" into table cust_indi;

<h3>Creating Table ext_cust from CTAS statement from cust_ind table and transforming the XML data into array<string> </h3>
create table ext_cust as select customerid,territoryid,accountnumber,customertype,modifieddate,
xpath(demographics,'IndividualSurvey/TotalPurchaseYTD/text()'),
xpath(demographics,'IndividualSurvey/DateFirstPurchase/text()'),
xpath(demographics,'IndividualSurvey/BirthDate/text()'),
xpath(demographics,'IndividualSurvey/MaritalStatus/text()'),
xpath(demographics,'IndividualSurvey/YearlyIncome/text()'),
xpath(demographics,'IndividualSurvey/Gender/text()'),
xpath(demographics,'IndividualSurvey/TotalChildren/text()'),
xpath(demographics,'IndividualSurvey/NumberChildrenAtHome/text()'),
xpath(demographics,'IndividualSurvey/Education/text()'),
xpath(demographics,'IndividualSurvey/Occupation/text()'),
xpath(demographics,'IndividualSurvey/HomeOwnerFlag/text()'),
xpath(demographics,'IndividualSurvey/NumberCarsOwned/text()'),
xpath(demographics,'IndividualSurvey/CommuteDistance/text()') 
from cust_indi;


<h3>Importing creditcard table From Mysql Adventureworks Database to HDFS Using Sqoop</h3>
sqoop import --connect jdbc:mysql://localhost:3306/adventureworks?useSSL=False \<br>
--username root --password-file file:///home/saif/LFS/cohort_c9/envvar/sqoop.pwd \<br>
--delete-target-dir --target-dir /user/saif/HFS/Input/adventureworks1\<br>
--query "select CreditCardID,CardType,CardNumber,ExpMonth,ExpYear,ModifiedDate from creditcard \<br>
where \$CONDITIONS" -m 1


<h3>Create manage table for credit card & Load data in manage table for credit card table</h3>
create table credit_hive<br>
(<br>
CreditCardID  int,<br>
CardType string,<br>
CardNumber bigint,<br>
ExpMonth int,<br>
ExpYear int,<br>
ModifiedDate timestamp<br>
) <br>
row format delimited fields terminated by ','  <br>
tblproperties("skip.header.line.count"="1");<br>
 
load data inpath "/user/saif/HFS/Input/adventureworks/" into table credit_hive;

<h3>Load data from hive to Pyspark & perform Transformation</h3>
 

<h3>Load data from Pyspark to hive and perform analytics</h3>
