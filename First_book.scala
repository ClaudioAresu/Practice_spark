// Databricks notebook source
val customer = spark.read
.option("header", "true")
.csv("/FileStore/tables/retailer/data/customer.csv")

// COMMAND ----------

display(customer)

// COMMAND ----------

val sameBirthMonth = customer.filter($"c_birth_month" === 7)

// COMMAND ----------

display(sameBirthMonth)

// COMMAND ----------

sameBirthMonth.count

// COMMAND ----------

val sameBirthDay = customer.filter($"c_birth_day" === 11)

// COMMAND ----------

sameBirthDay.count

// COMMAND ----------

display(sameBirthDay)

// COMMAND ----------

