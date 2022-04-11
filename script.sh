#!/bin/bash

. script.env

#Importing Customer_individual view From Mysql Adventureworks Database to HDFS Using Sqoop
sqoop import --connect jdbc:mysql://$host:$port/$db?useSSL=False \
--username $user --password-file $pass_file \
--delete-target-dir --target-dir /user/saif/HFS/Input/adventureworks \
--query $query -m 1

#Creation Of Hive manage table & Load data from hdfs in manage table


hive -e "create table adventureworks.cust_ind
(
CustomerID int,
TerritoryID int,
AccountNumber string,
CustomerType string,
ModifiedDate timestamp,
Demographics string
)
row format delimited fields terminated by ','
tblproperties("skip.header.line.count"="1") ;

load data inpath '/user/saif/HFS/Input/adventureworks/' into table cust_ind;"

#Creating Table ext_cust from CTAS statement from cust_ind table and transforming the XML data into array

hive -e "create table ext_cust 
as 
select customerid,territoryid,accountnumber,customertype,modifieddate,
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
 from cust_indi;"

#Importing creditcard table From Mysql Adventureworks Database to HDFS Using Sqoop

sqoop import --connect jdbc:mysql://$host:$port/$db?useSSL=False \
--username $user --password-file $pass_file \
--delete-target-dir --target-dir /user/saif/HFS/Input/adventureworks1\
--query "select CreditCardID,CardType,CardNumber,ExpMonth,ExpYear,ModifiedDate from creditcard \
where \$CONDITIONS" -m 1

#Create manage table for credit card & Load data in manage table for credit card table

hive -e "create table credit_hive
(
CreditCardID int,
CardType string,
CardNumber bigint,
ExpMonth int,
ExpYear int,
ModifiedDate timestamp
)
row format delimited fields terminated by ','
tblproperties("skip.header.line.count"="1");

load data inpath "/user/saif/HFS/Input/adventureworks/" into table credit_hive;"

#Load data from hive to Pyspark & perform Transformation
#Load data from Pyspark to hive and perform analytics
spark-submit pyspark-transformation.py
