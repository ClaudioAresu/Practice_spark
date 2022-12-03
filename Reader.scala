// Databricks notebook source
val CustomerDf = spark.read
.format("csv")
.options(Map("header" -> "true",
             "delimiter" -> ",",
             "inferSchema" -> "false"))
.schema("c_customer_sk long,c_customer_id string,c_current_cdemo_sk long,c_current_hdemo_sk long,"+
"c_current_addr_sk long,c_first_shipto_date_sk long,c_first_sales_date_sk long,c_salutation string,c_first_name string,"+
"c_last_name string,c_preferred_cust_flag string,c_birth_day int,c_birth_month int,c_birth_year int,c_brth_country string,"+
"c_login string,c_email_address string,c_last_review_date long")
.load("/FileStore/tables/retailer/data/customer.csv")

// COMMAND ----------

import org.apache.spark.sql.types._

// COMMAND ----------

val householdType = StructType(
  Array(
    StructField("hd_demo_sk",IntegerType,true),
    StructField("hd_income_band_sk",LongType,true),
    StructField("hd_buy_potential",StringType,true),
    StructField("hd_dep_count",IntegerType,true),
    StructField("hd_vehicle_count",IntegerType)
  )
)

// COMMAND ----------

//mode can be switched to failfast to throw an exception or dropmalformed to remove the null values
val household_demoDf = spark.read
.schema(householdType)

.options(
  Map("header" -> "true",
       "sep" -> "|",
       "badRecordsPath" -> "/tmp/badRecordsPath"))
.csv("/FileStore/tables/retailer/data/household_demographics.dat")

// COMMAND ----------

display(household_demoDf)

// COMMAND ----------

// MAGIC %fs head /FileStore/tables/retailer/data/household_demographics.dat

// COMMAND ----------



// COMMAND ----------

val userDf = spark.read
.format("json")
.schema(userDfSchema)
.option("dateformat","dd.MM.yyyy")
.option("path","/FileStore/tables/retailer/data/single_line.json")
.load

// COMMAND ----------

display(userDf)

// COMMAND ----------

val userDfSchema = """address struct
<city: string,
country: string,
state: string>,
birthday date,
email string,
first_name string,
id long,
last_name string,
skills array<string>"""

// COMMAND ----------

// MAGIC %fs head /FileStore/tables/multi_line.json

// COMMAND ----------

val userDf = spark.read
.format("json")
.schema(userDfSchema)
.option("dateformat","dd.MM.yyyy")
.option("path","/FileStore/tables/multi_line.json")
.option("multiline",true)
.load

// COMMAND ----------

val df = userDf.select('id,'first_name,$"address.city",'address,'skills(0),'skills)

// COMMAND ----------

display(df)