// Databricks notebook source
// MAGIC %run ./Schema_Declarations

// COMMAND ----------

val customerDf = spark.read
.schema(customerSchemaDDL)
.options(Map("header" ->"true",
            "delimiter" -> "|",
            "inferSchema" -> "false"))
.csv("/FileStore/tables/retailer/data/customer.dat")

// COMMAND ----------

val CleanseBirth = customerDf
.filter($"c_birth_month".isNotNull)


// COMMAND ----------

display(CleanseBirth)

// COMMAND ----------

def getCustomerBirthDate(year:Int,day:Int,month:Int) : String = {
  return java.time.LocalDate.of(year,day,month).toString
}

// COMMAND ----------

val getCustomerBirthDate_udf =
udf(getCustomerBirthDate(_:Int,_:Int,_:Int):String)

// COMMAND ----------

import org.apache.spark.sql.functions._

// COMMAND ----------

customerDf.select(
 getCustomerBirthDate_udf($"c_birth_year",$"c_birth_day",$"c_birth_month").as("c_birth_date"),
  $"c_customer_sk",
  concat($"c_first_name",$"c_last_name").as("c_name"),
  $"c_birth_year",
  $"c_birth_day",
  $"c_birth_month"
).show(false)

// COMMAND ----------

customerDf.createOrReplaceTempView("vCustomer")

// COMMAND ----------

// MAGIC %sql
// MAGIC select
// MAGIC   c_customer_sk,
// MAGIC   concat(c_first_name,c_last_name) as c_name,
// MAGIC   c_birth_year,
// MAGIC   c_birth_month,
// MAGIC   c_birth_day,
// MAGIC   getCustomerBirthDate_udf(c_birth_year,c_birth_day,c_birth_month) as c_birth_date
// MAGIC from vCustomer

// COMMAND ----------

