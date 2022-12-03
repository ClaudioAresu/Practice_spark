// Databricks notebook source
val customer = spark.read
.option("header","true")
.csv("/FileStore/tables/retailer/customer.csv")

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

// MAGIC %md
// MAGIC ##Activity 
// MAGIC <br>
// MAGIC From the customer DataFrame do the following:
// MAGIC 
// MAGIC   * Find all customers that were born on the same Day, Month and Year as you
// MAGIC   * Count the number of customers that were born in the same year as you