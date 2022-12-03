// Databricks notebook source
val customer = spark.read
.option("header", "true")
.csv("/FileStore/tables/retailer/data/customer.csv")

// COMMAND ----------

display(customer)

// COMMAND ----------

customer.count

// COMMAND ----------

val sameBirthMonth = customer.filter($"c_birth_month" === 1)

// COMMAND ----------

sameBirthMonth.count

// COMMAND ----------

display(sameBirthMonth)

// COMMAND ----------

