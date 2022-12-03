// Databricks notebook source
val customerAddressDf = spark.read
.option("inferSchema", true)
.option("header", true)
.option("sep", "|")
.csv("/FileStore/tables/retailer/data/customer_address.dat")

// COMMAND ----------

val customerDf = spark.read
.option("inferSchema", true)
.option("header", true)
.csv("/FileStore/tables/retailer/data/customer.csv")

// COMMAND ----------

display(customerDf)

// COMMAND ----------

val joinExpression = customerDf.col("c_current_addr_sk") === customerAddressDf.col("ca_address_sk")

// COMMAND ----------

val customerWithAddressDf = customerDf
.join(customerAddressDf,joinExpression,"inner")
.select("c_customer_id",
        "c_first_name",
       "c_last_name",
       "ca_address_sk",
       "c_salutation",
       "ca_country",
       "ca_city",
       "ca_street_name",
       "ca_zip"
       "ca_street_number")

// COMMAND ----------

customerWithAddressDf.show

// COMMAND ----------

display(customerWithAddressDf)

// COMMAND ----------

customerDf.count()

// COMMAND ----------

import org.apache.spark.sql.functions.count

// COMMAND ----------

customerDf.select(count("*")).show

// COMMAND ----------

//select column to not count Null values
customerDf.select(count("c_first_name")).show

// COMMAND ----------

customerDf.filter($"c_first_name".isNotNull).count

// COMMAND ----------

import org.apache.spark.sql.functions.countDistinct

// COMMAND ----------

customerDf.select(countDistinct("c_first_name")).show

// COMMAND ----------

val itemDf = spark.read
.format("csv")
.options(Map("header" -> "true"
            ,"delimiter" -> "|"
            ,"inferSchema" -> "true"))
.load("/FileStore/tables/retailer/data/item.dat")

// COMMAND ----------

// DBTITLE 1,Item DataFrame
display(itemDf)

// COMMAND ----------

import org.apache.spark.sql.functions.min

// COMMAND ----------

val minWSCostDf= itemDf.select(min("i_wholesale_cost")).withColumnRenamed("min(i_wholesale_cost)","minWSCost")

// COMMAND ----------

minWSCostDf.show

// COMMAND ----------

display(itemDf.filter($"i_wholesale_cost" === 0.02))

// COMMAND ----------

val cheapestItemDf = itemDf.join(minWSCostDf,itemDf.col("i_wholesale_cost") === minWSCostDf.col("minWSCost"))

// COMMAND ----------

//merging the wholesale cost and min cost table to find all the data of the specified sale

display(cheapestItemDf)

// COMMAND ----------

import org.apache.spark.sql.functions.max

// COMMAND ----------

val MaxWSCostDf = itemDf
.select(max("i_wholesale_cost"))
.withColumnRenamed("max(i_wholesale_cost)","max_item_cost")

// COMMAND ----------

display(MaxWSCostDf)

// COMMAND ----------

val ExpensiveItemDf = itemDf.join(MaxWSCostDf,itemDf.col("i_wholesale_cost") === MaxWSCostDf.col("max_item_cost") )

// COMMAND ----------

display(ExpensiveItemDf)

// COMMAND ----------

val MaxCPriceDf = itemDf
.select(max("i_current_price"))
.withColumnRenamed("max(i_current_price)","max_current_price")

// COMMAND ----------

display(MaxCPriceDf)

// COMMAND ----------

val storesSalesDf = spark.read
.format("csv")
.options(Map("header" -> "true"
            , "delimiter" -> "|"
            , "inferSchema" -> "true"))
.load("/FileStore/tables/store_sales.dat")

// COMMAND ----------

import org.apache.spark.sql.functions.{sum,sum_distinct,avg,mean}

// COMMAND ----------

storesSalesDf.select(sum("ss_net_paid_inc_tax"),sum("ss_net_profit")).show

// COMMAND ----------

storesSalesDf.select(sum_distinct($"ss_quantity")).show

// COMMAND ----------

storesSalesDf.count

// COMMAND ----------

storesSalesDf.select(
avg("ss_quantity").as("average_purchases")
  , sum("ss_quantity") / count("ss_quantity")
  , min("ss_quantity").as("min_purchases")
  , max("ss_quantity").as("max_quantity")
).show

// COMMAND ----------

 storesSalesDf.groupBy("ss_customer_sk").agg(
   countDistinct("ss_item_sk").as("item_count")
   , sum("ss_quantity").as("total_items")
   , sum("ss_net_paid").as("net_income")
   , max("ss_net_paid").as("max_snet_paid")
   , min("ss_net_paid").as("min_net_paid")
   , avg("ss_net_paid").as("avg_net_paid")
   , max("ss_sales_price").as("max_sale")
   , min("ss_sales_price").as("min_sale")
   , avg("ss_sales_price").as("avg_sale")
 )

// COMMAND ----------

display(res71)