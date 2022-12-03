// Databricks notebook source
DataFrameWriter
.format
.option
.partitionBy
.save

// COMMAND ----------

val CustomerDf = spark.read
.options(Map("header" -> "true",
             "inferSchema" -> "false"))
.schema("c_customer_sk long,c_customer_id string,c_current_cdemo_sk long,c_current_hdemo_sk long,"+
"c_current_addr_sk long,c_first_shipto_date_sk long,c_first_sales_date_sk long,c_salutation string,c_first_name string,"+
"c_last_name string,c_preferred_cust_flag string,c_birth_day int,c_birth_month int,c_birth_year int,c_birth_country string,"+
"c_login string,c_email_address string,c_last_review_date long")
.csv("/FileStore/tables/retailer/data/customer.csv")

// COMMAND ----------

val df = CustomerDf.selectExpr("c_customer_id as id","c_first_name as first_name","c_last_name as last_name","c_birth_year as birth_year","c_birth_month as birth_month")

// COMMAND ----------

display(df)

// COMMAND ----------

df.write
.format("csv")
.mode(SaveMode.Overwrite)
.option("path","/tmp/output_csv")
.option("sep","|")
.option("header", true)
.save

// COMMAND ----------

// MAGIC  %fs ls /tmp/output_csv

// COMMAND ----------

// MAGIC %fs head /tmp/output_csv/part-00000-tid-138216076330709025-0f4e3a1c-a4af-4e09-8143-5abae204ba61-41-1-c000.csv

// COMMAND ----------

df.rdd.getNumPartitions

// COMMAND ----------

df.write
.format("json")
.mode(SaveMode.Overwrite)
.option("path","/tmp/output_json")
.save

// COMMAND ----------

// MAGIC  %fs ls /tmp/output_json

// COMMAND ----------

df.write.saveAsTable("default.customer_tbl")

// COMMAND ----------

// MAGIC %sql
// MAGIC describe formatted default.customer_tbl

// COMMAND ----------

// MAGIC %fs ls 
// MAGIC dbfs:/user/hive/warehouse/customer_tbl

// COMMAND ----------

// MAGIC %sql
// MAGIC select * from default.customer_tbl

// COMMAND ----------

// MAGIC %sql
// MAGIC drop table default.customer_tbl

// COMMAND ----------

df.write
.format("csv")
.mode(SaveMode.Overwrite)
.partitionBy("birth_year")
.option("path","/tmp/output_csv")
.option("sep","|")
.option("header", true)
.save

// COMMAND ----------

// MAGIC  %fs ls /tmp/output_csv

// COMMAND ----------

display(spark.read
.option("sep","|")
.option("header", true)
.csv("dbfs:/tmp/output_csv/birth_year=1931/")
        )

// COMMAND ----------

val personSeq =
Seq(
  (0,"Javier","Lewis"),
  (1,"Amy","Moses")
  )

// COMMAND ----------

val personDf = personSeq.toDF("id","first_name","last_name")

// COMMAND ----------

display(personDf)

// COMMAND ----------

personDf.printSchema

// COMMAND ----------

val personRows = Seq(
  Row  (0L,"Javier","Lewis"),
  Row(1L,"Amy","Moses")
)

// COMMAND ----------

import org.apache.spark.sql.types._

// COMMAND ----------

val personSchema = new StructType(Array(
  new StructField("person_id",LongType,false),
  new StructField("first_name",StringType,true),
  new StructField("last_name",StringType,true)
))

// COMMAND ----------

spark.createDataFrame(spark.sparkContext.parallelize(personRows),personSchema)

// COMMAND ----------

display(res36)

// COMMAND ----------

res36.repartition(1)

// COMMAND ----------

res43.rdd.getNumPartitions