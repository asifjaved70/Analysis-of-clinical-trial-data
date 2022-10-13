-- Databricks notebook source
-- MAGIC %md This is a reusable notebook. By changing the value of variables it performs operations according ↑ 

-- COMMAND ----------

-- MAGIC %md Making the environment to extract the desired conclusions 

-- COMMAND ----------

-- MAGIC %python
-- MAGIC dbutils.widgets.text(name = "fileroot", defaultValue = "clinicaltrial_2021.csv", label= 'File Name')
-- MAGIC dbutils.widgets.text(name = "Year", defaultValue = "2021")

-- COMMAND ----------

--DROP TABLE clinicaltrial_2021;
CREATE EXTERNAL TABLE IF NOT EXISTS clinicaltrial_2021 
( Id STRING,
Sponsor STRING,
Status STRING,
Start STRING,
Completion STRING,
Type STRING,
Submission STRING,
Conditions STRING,
Interventions STRING ) 
USING CSV
OPTIONS (path 'dbfs:/FileStore/tables/$fileroot',
        delimiter "|",
        header "true");
--LOAD DATA LOCAL INPATH "dbfs:/FileStore/clinicaltrial_2019_csv" OVERWRITE INTO TABLE clinicaltrial_2021;

-- COMMAND ----------

-- MAGIC %md Question 1 : The number of studies in the dataset. You must ensure that you explicitly check distinct studies.

-- COMMAND ----------

select Distinct count(*) from clinicaltrial_2021;

-- COMMAND ----------

-- MAGIC %md Question 2:  You should list all the types (as contained in the Type column) of studies in the dataset along with
-- MAGIC the frequencies of each type. These should be ordered from most frequent to least frequent.

-- COMMAND ----------

select Type,count(Type) as count from clinicaltrial_2021 group by Type order by count desc limit 4

-- COMMAND ----------

-- MAGIC %md Question 3: The top 5 conditions (from Conditions) with their frequencies.

-- COMMAND ----------

CREATE OR REPLACE VIEW  ConditionSplit_view 
AS 
SELECT id, explode_codition
  FROM clinicaltrial_2021
  lateral VIEW explode(split(Conditions,",")) condition_view AS explode_codition
  where   length(explode_codition) > 0 ;

-- COMMAND ----------

select explode_codition,count(explode_codition) as count 
from ConditionSplit_view 
group by explode_codition 
order by count desc limit 6

-- COMMAND ----------

-- MAGIC %md Question 4:  Each condition can be mapped to one or more hierarchy codes. The client wishes to know the 5
-- MAGIC most frequent roots (i.e. the sequence of letters and numbers before the first full stop) after this is
-- MAGIC done.

-- COMMAND ----------

CREATE External TABLE IF NOT EXISTS mesh (
term STRING ,
tree STRING
) 
USING CSV
OPTIONS (path "dbfs:/FileStore/tables/mesh.csv",
        delimiter ",",
        header "true")
        ;

-- COMMAND ----------

CREATE OR REPLACE VIEW  disease_code
As
select term,tree,substr(tree,1,3) as disease_code from mesh

-- COMMAND ----------

CREATE OR REPLACE VIEW  joinDiseasecodeView 
As
SELECT c.id,c.explode_codition,d.disease_code
FROM ConditionSplit_view as c
JOIN disease_code as d ON c.explode_codition=d.term

-- COMMAND ----------

select 
disease_code,count(disease_code) as count from joinDiseasecodeView group by 
disease_code order by count desc limit 10

-- COMMAND ----------

-- MAGIC %md Question 5: Find the 10 most common sponsors that are not pharmaceutical companies, along with the number
-- MAGIC of clinical trials they have sponsored. Hint: For a basic implementation, you can assume that the
-- MAGIC Parent Company column contains all possible pharmaceutical companies.

-- COMMAND ----------

CREATE External TABLE IF NOT EXISTS pharma (
Company STRING,
Parent_Company STRING, 
Penalty_Amount STRING, 
Subtraction_From_Penalty STRING, 
Penalty_Amount_Adjusted_For_Eliminating_Multiple_Counting STRING, 
Penalty_Year STRING, 
Penalty_Date STRING, 
Offense_Group STRING, 
Primary_Offense STRING, 
Secondary_Offense STRING, 
Description STRING,
Level_of_Government STRING, 
Action_Type STRING, 
Agency STRING, 
Civil_Criminal STRING, 
Prosecution_Agreement STRING,
Court STRING,
Case_ID STRING,
Private_Litigation_Case_Title STRING,
Lawsuit_Resolution STRING,
Facility_State STRING,
City STRING,
Address STRING,
Zip STRING,
NAICS_Code STRING,
NAICS_Translation STRING,
HQ_Country_of_Parent STRING,
HQ_State_of_Parent STRING,
Ownership_Structure STRING,
Parent_Company_Stock_Ticker STRING,
Major_Industry_of_Parent STRING,
Specific_Industry_of_Parent STRING,
Info_Source STRING,
Notes STRING
) 
USING CSV
OPTIONS (path "dbfs:/FileStore/tables/pharma.csv",
        delimiter ",",
        header "true")
        ;

-- COMMAND ----------

CREATE OR REPLACE VIEW  joinPharmaCompany
AS
select c.*,p.Parent_Company
from clinicaltrial_2021 as c LEFT OUTER JOIN pharma p 
ON (c.Sponsor=p.Parent_Company)

-- COMMAND ----------

select 
Sponsor,count(Sponsor)as count 
from joinPharmaCompany
 where
Parent_Company is null group by sponsor order by count(Sponsor) desc Limit 10

-- COMMAND ----------

-- MAGIC %md Question 6: Plot number of completed studies each month in a given year – for the submission dataset, the year
-- MAGIC is 2021. You need to include your visualization as well as a table of all the values you have plotted
-- MAGIC for each month.

-- COMMAND ----------

CREATE OR REPLACE VIEW  month_year
AS
select *, split(completion,' ')[0] as Month, split(completion,' ')[1] as Year from clinicaltrial_2021 where completion is not null

-- COMMAND ----------

CREATE OR REPLACE VIEW  counting_view
AS
select month,count(month)as count from month_year where Status=='Completed' and Year=='$Year' group by month

-- COMMAND ----------

CREATE OR REPLACE VIEW final_month_view AS
SELECT month, count,
CASE 
WHEN month ='Jan' THEN 1 
WHEN month ='Feb' THEN 2 
WHEN month ='Mar' THEN 3 
WHEN month ='Apr' THEN 4 
WHEN month ='May' THEN 5 
WHEN month ='Jun' THEN 6 
WHEN month ='Jul' THEN 7 
WHEN month ='Aug' THEN 8 
WHEN month ='Sep' THEN 9 
WHEN month ='Oct' THEN 10 
WHEN month ='Nov' THEN 11 
WHEN month ='Dec' THEN 12
END as month_sorting
FROM counting_view ORDER by month_sorting asc;

-- COMMAND ----------

select month,count from final_month_view

-- COMMAND ----------

select month,count from final_month_view

-- COMMAND ----------

select month,count from final_month_view
