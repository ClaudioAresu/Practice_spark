// Databricks notebook source
val customerDf = spark.read
.option("inferSchema", true)
.option("sep", "|")
.option("header", true)
.csv("/FileStore/tables/retailer/data/customer.dat")

// COMMAND ----------

display(customerDf)

// COMMAND ----------

val customerWithValidBirthInfo = customerDf
.filter($"c_birth_day" > 0)
.filter($"c_birth_day" <= 31)
.filter($"c_birth_month" > 0)
.filter($"c_birth_month" <= 12)
.filter($"c_birth_year".isNotNull)
.filter($"c_birth_year" > 0)

// COMMAND ----------

customerWithValidBirthInfo
.where($"c_birth_day" === 1)

// COMMAND ----------

display(res10)

// COMMAND ----------

//check for inequality
customerWithValidBirthInfo
.where($"c_birth_day" =!= 1)

// COMMAND ----------

display(res15)

// COMMAND ----------

