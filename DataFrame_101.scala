// Databricks notebook source
// MAGIC %fs ls /FileStore/tables/retailer/data

// COMMAND ----------

val customerDF = spark.read.csv("dbfs:/FileStore/tables/retailer/data/customer.csv")

// COMMAND ----------

val customerDf1 = spark.read.format("csv").load("dbfs:/FileStore/tables/retailer/data/customer.csv")

// COMMAND ----------

display(customerDf1)

// COMMAND ----------

val customerDfWithHeader = spark
.read
.option("header",true)
.csv("dbfs:/FileStore/tables/retailer/data/customer.csv")

// COMMAND ----------

display(customerDfWithHeader)

// COMMAND ----------

// MAGIC %fs head dbfs:/FileStore/tables/retailer/data/customer.dat

// COMMAND ----------

val customerDfWithHeader1 = spark
.read
.option("header",true)
.option("sep","|")
.csv("dbfs:/FileStore/tables/retailer/data/customer.dat")

// COMMAND ----------

display(customerDfWithHeader1)

// COMMAND ----------

customerDfWithHeader.columns.size

// COMMAND ----------

val customerWithBirthDay = customerDfWithHeader.select("c_customer_id",
                                                      "c_first_name",
                                                      "c_last_name",
                                                      "c_birth_year",
                                                      "c_birth_month",
                                                      "c_birth_day")

// COMMAND ----------

customerWithBirthDay.show

// COMMAND ----------

import org.apache.spark.sql.functions.{col,column}

// COMMAND ----------

val customerWithBirthDate = customerDfWithHeader.select(col("c_customer_id"),
                                                       col("c_first_name"),
                                                       column("c_last_name"),
                                                       $"c_birth_year",
                                                       'c_birth_month,
                                                       $"c_birth_day")

// COMMAND ----------

display(customerWithBirthDate)

// COMMAND ----------

customerWithBirthDate.printSchema

// COMMAND ----------

customerWithBirthDate.schema

// COMMAND ----------

customerDfWithHeader.printSchema

// COMMAND ----------

val customerDf = spark.read
.option("header", true)
.option("inferSchema", true)
.csv("dbfs:/FileStore/tables/retailer/data/customer.csv")

// COMMAND ----------

customerDf.printSchema

// COMMAND ----------

// MAGIC %fs ls /FileStore/tables/retailer/data

// COMMAND ----------

// MAGIC %fs head dbfs:/FileStore/tables/retailer/data/customer_address.dat

// COMMAND ----------

val customerAddressDf = spark.read
.option("header", true)
.option("sep","|")
.option("inferSchema", true)
.csv("dbfs:/FileStore/tables/retailer/data/customer_address.dat")

// COMMAND ----------

customerAddressDf.printSchema

// COMMAND ----------

val customerAddressSchemaDDL = "ca_address_sk long, ca_address_id string, ca_street_number string,"+"ca_street_name string, ca_street_tyype string,ca_suite_number string, ca_city string ,ca_county string,"+"ca_state string , ca_zip string, ca_gmt_offset decimal(5,2) , ca_location_type string"

// COMMAND ----------

val customerAddressDf = spark.read
.option("header", true)
.option("sep","|")
.schema(customerAddressSchemaDDL)
.csv("dbfs:/FileStore/tables/retailer/data/customer_address.dat")

// COMMAND ----------

customerAddressDf.printSchema

// COMMAND ----------

customerAddressDf.schema.toDDL

// COMMAND ----------

