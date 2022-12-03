// Databricks notebook source
// MAGIC %run ./Schema_Declarations

// COMMAND ----------

import org.apache.spark.sql.functions.concat

// COMMAND ----------

val CustomerDf = spark.read
.schema(customerSchemaDDL)
.options(Map("header" -> "true",
             "delimiter" -> "|",
             "inferSchema" -> "false"))
.csv("/FileStore/tables/retailer/data/customer.dat")

// COMMAND ----------

val CustomerAddressDf = spark.read
.schema(customerAddressSchema)
.options(Map("header" -> "true",
             "delimiter" -> "|",
             "inferSchema" -> "false"))
.csv("/FileStore/tables/retailer/data/customer_address.dat")

// COMMAND ----------

val HouseholdDemoDf = spark.read
.format("csv")
.schema(HouseHoldDemoSchema)
.options(Map("header" -> "true",
             "delimiter" -> "|",
             "inferSchme" -> "false"))
.csv("/FileStore/tables/retailer/data/household_demographics.dat")

// COMMAND ----------

val IncomeBandDf = spark.read
.format("csv")
.schema(IncomeBandSchema)
.options(Map("header" -> "true",
             "delimiter" -> "|",
             "inferSchema" -> "false"))
.csv("/FileStore/tables/income_band.dat")

// COMMAND ----------

val result = CustomerDf.join(HouseholdDemoDf,
               CustomerDf.col("c_current_hdemo_sk") === HouseholdDemoDf.col("hd_demo_sk"))
.join(IncomeBandDf,
     IncomeBandDf.col("ib_income_band_sk") === HouseholdDemoDf.col("hd_income_band_sk"))

.where($"ib_lower_bound" >= 38128)
.where($"ib_upper_bound" <= 88128)

.join(CustomerAddressDf,
     CustomerDf.col("c_current_addr_sk") === CustomerAddressDf.col("ca_address_sk"))
.where($"ca_city" === "Edgewood")
.select($"c_customer_id",
       $"c_customer_sk",
       concat($"c_first_name",$"c_last_name").as("customerName"),
       $"ca_city",
       $"ib_lower_bound",
       $"ib_upper_bound",
       )

// COMMAND ----------

result.printSchema

// COMMAND ----------

display(result)