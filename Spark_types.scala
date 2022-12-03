// Databricks notebook source


// COMMAND ----------



// COMMAND ----------

val household_demographicsDf = spark.read
.schema(
  "hd_demo_sk long, hd_income_band_sk long, hd_buy_potential string, hd_dep_count int, hd_vehicle_count int")
.options(Map(
  "header" -> "true",
  "sep" -> "|"))
.csv("/FileStore/tables/retailer/data/household_demographics.dat")

// COMMAND ----------

household_demographicsDf
.where($"hd_demo_sk".isNotNull)

// COMMAND ----------

display(household_demographicsDf)

// COMMAND ----------

import org.apache.spark.sql.types._

// COMMAND ----------

val hds = StructType(
  Array( 
      StructField("hd_demo_sk",LongType,true),
      StructField("hd_income_band_sk",LongType,true),
      StructField("hd_buy_potential",StringType,true),
      StructField("hd_dep_count",IntegerType,true),
      StructField("hd_vehicle_count",IntegerType,true)
    )
)


// COMMAND ----------

val household_demographicsDf = spark.read
.schema(hds)
.options(Map(
  "header" -> "true",
  "sep" -> "true"))
.csv("/FileStore/tables/retailer/data/household_demographics.dat")

// COMMAND ----------

household_demographicsDf.printSchema

// COMMAND ----------

import org.apache.spark.sql.functions.lit


// COMMAND ----------

household_demographicsDf.select($"*",lit("house").name("Mansion")).schema

// COMMAND ----------

household_demographicsDf.filter($"hd_income_band_sk" === 2).show(5)
// household_demographicsDf.filter($"hd_income_band_sk".equalTo(2)).show(5)

// COMMAND ----------

household_demographicsDf.filter($"hd_income_band_sk".notEqual(2)).show(5)
//household_demographicsDf.filter($"hd_income_band_sk". =!= 2).show(5)
//household_demographicsDf.filter($"hd_income_band_sk". <> 2).show(5)

// COMMAND ----------

household_demographicsDf
  .where($"hd_income_band_sk".notEqual(2))
  .where($"hd_vehicle_count" > 3 or $"hd_vehicle_count" === 1)
  .orderBy($"hd_vehicle_count".desc)
  .show

// COMMAND ----------

household_demographicsDf.select("hd_buy_potential").distinct.show

// COMMAND ----------

import org.apache.spark.sql.functions.trim

// COMMAND ----------

val isBuyPotentialHigh = trim($"hd_buy_potential").equalTo("5001-10000")

// COMMAND ----------

val Df = household_demographicsDf.withColumn("high_buying_potential",isBuyPotentialHigh)

// COMMAND ----------

Df
  .select($"*")
  .orderBy($"high_buying_potential".desc)
  .show

// COMMAND ----------

Df
.where($"high_buying_potential".or($"hd_vehicle_count" > 3))
.orderBy("hd_vehicle_count")
.show
// you can directly filter a df if the column is of boolean type

// COMMAND ----------

import org.apache.spark.sql.functions.least

// COMMAND ----------

// MAGIC %sql
// MAGIC 
// MAGIC create database retailer_db

// COMMAND ----------

// MAGIC %sql
// MAGIC 
// MAGIC create table if not exists retailer_db.web_sales
// MAGIC using csv
// MAGIC options(
// MAGIC path '/FileStore/tables/web_sales.dat',
// MAGIC sep '|',
// MAGIC header true
// MAGIC )

// COMMAND ----------

val webSalesDf = spark.read
.table("retailer_db.web_sales")

// COMMAND ----------

display(webSalesDf)

// COMMAND ----------

val salesStatDf = webSalesDf.select(
  "ws_order_number",
  "ws_item_sk",
  "ws_quantity",
  "ws_net_profit",
  "ws_net_paid",
  "ws_wholesale_cost"
)

// COMMAND ----------

import org.apache.spark.sql.functions.{expr,round,bround}

// COMMAND ----------

val salesPerfDf = salesStatDf
.withColumn("expected_net_paid", $"ws_quantity"*$"ws_wholesale_cost")
.withColumn("calculated_profit", $"ws_net_paid" - $"expected_net_paid")
.withColumn("unit_price", expr("ws_wholesale_cost / ws_quantity"))
.withColumn("rounded_unitPrice",round($"unit_price",2))
.withColumn("brounded_unitPrice",bround($"unit_price",2))

// COMMAND ----------

display(salesPerfDf)

// COMMAND ----------

salesPerfDf.describe()

// COMMAND ----------

display(res68)

// COMMAND ----------

// MAGIC %sql
// MAGIC 
// MAGIC create table if not exists retailer_db.item
// MAGIC using csv
// MAGIC options(
// MAGIC path '/FileStore/tables/retailer/data/item.dat',
// MAGIC sep '|',
// MAGIC header true
// MAGIC )

// COMMAND ----------

val itemDf = spark.read.table("retailer_db.item")

// COMMAND ----------

display(itemDf)

// COMMAND ----------

itemDf
.where($"i_item_desc".contains("young"))

// COMMAND ----------

display(res76)

// COMMAND ----------

// MAGIC %sql
// MAGIC select count(i_item_sk) from retailer_db.item where i_item_desc like "%young%"

// COMMAND ----------

itemDf

// COMMAND ----------

import org.apache.spark.sql.functions.{length}

// COMMAND ----------

itemDf.select(length(trim($"i_color"))).show

// COMMAND ----------

// MAGIC %sql
// MAGIC select distinct(i_color) from retailer_db.item

// COMMAND ----------

// MAGIC %sql
// MAGIC select length(i_color) from retailer_db.item

// COMMAND ----------

import org.apache.spark.sql.functions.{rtrim,ltrim}

// COMMAND ----------

// MAGIC %sql
// MAGIC select i_item_desc,
// MAGIC initcap(i_item_desc),
// MAGIC lower(i_item_desc),
// MAGIC upper(i_item_desc)
// MAGIC 
// MAGIC from retailer_db.item

// COMMAND ----------

import org.apache.spark.sql.functions.{lower,upper,initcap}

// COMMAND ----------

itemDf.select(lower($"i_item_desc"))

// COMMAND ----------

display(res85)

// COMMAND ----------

import org.apache.spark.sql.functions.{lpad,rpad}

// COMMAND ----------

itemDf.where($"i_color" === rpad(lit("pink"),20, " ")).count

// COMMAND ----------



// COMMAND ----------

import org.apache.spark.sql.functions.{to_date,lit,unix_timestamp,from_unixtime,date_sub,datediff,months_between}
import org.apache.spark.sql.types.{TimestampType}

// COMMAND ----------

//examples of conversion to and form date and timestamp
//to_date(lit("1999-07-09"))
//to_date(lit("1999/07/09"),"yyyy/MM/dd"))
//to_date(lit("1999.07.09"),"yyyy.MM.dd"))
//to_date(lit("1999.07.09 18:15:00"), "yyyy.MM.dd HH:mm:ss"))
//to_date(lit("1999.07.09 06:15:00 PM"), "yyyy.MM.dd HH:mm:ss a"))
//unix_timestamp(lit("1999.07.09 18:15:00"), "yyyy.MM.dd HH:mm:ss"))
//from_unixtime($"sold_date_7","dd.MM..yyyy")
//unix_timestamp(lit("1999.07.09 06:15:00 PM"), "yyyy.MM.dd HH:mm:ss a").cast(TimestampType))

// COMMAND ----------

//(column),date_add(columntype,int) = return a new column with a date equal to x days after a given date, where x is an int
//(column),date_sub(columntype,int) = return a new column with a date equal to x days before a given date, where x is an int
//(column),datediff(columntype,columntype) = return a new column with a difference in days of dates between two given columns
//(column),months_between(columntype,columntype) = return a new column with a difference in months of dates between two given columns

// COMMAND ----------

import org.apache.spark.sql.functions.{expr,struct}

// COMMAND ----------

webSalesDf.select($"ws_bill_customer_sk".as("customer_id"),
  struct($"ws_item_sk".as("item_id"),$"ws_quantity".as("quantity")).as("item_quantity"))

// COMMAND ----------

display(res105)

// COMMAND ----------

val itemQuantityDf = webSalesDf.selectExpr("ws_bill_customer_sk customer_id",
                                           "(ws_item_sk item_id, ws_quantity quantity) as item_quantity")

// COMMAND ----------

itemQuantityDf.select("customer_id", "item_quantity").show

// COMMAND ----------

itemQuantityDf.select("customer_id", "item_quantity.item_id").show

// COMMAND ----------

itemQuantityDf.select("item_quantity.*")

// COMMAND ----------

itemQuantityDf.createOrReplaceTempView("tempDf")

// COMMAND ----------

// MAGIC %sql
// MAGIC select item_quantity from tempDf limit 10

// COMMAND ----------

// MAGIC %sql
// MAGIC select customer_id, item_quantity.item_id from tempDf limit 10

// COMMAND ----------

import org.apache.spark.sql.functions.{collect_set,size,explode,max,array,array_contains}

// COMMAND ----------

val customerItensDf = webSalesDf
.groupBy("ws_bill_customer_sk")
.agg(
collect_set("ws_item_sk").as("itemList")
)

// COMMAND ----------

display(customerItensDf)

// COMMAND ----------

customerItensDf
.select($"*",
       size($"itemList")
        .as("item_quantity"),
       $"itemList"(0)
        .as("firstItem"),
       explode($"itemList"),
       array_contains($"itemList","13910")
        .as("hasItem")
       )
.where(!$"hasItem")
.show

// COMMAND ----------

webSalesDf.select($"ws_item_sk",
                 array($"ws_sold_date_sk",$"ws_ship_date_sk"))
.show

// COMMAND ----------

import org.apache.spark.sql.functions.{map}

// COMMAND ----------

val itemInfoDf = spark.read.table("retailer_db.item")
.select($"i_item_sk"
       ,map(trim($"i_category"),trim($"i_product_name")).as("category")
        )

// COMMAND ----------

itemInfoDf.printSchema

// COMMAND ----------



// COMMAND ----------

itemInfoDf.select($"i_item_sk"
                 ,$"category.Music".as("product_name")
                 ,lit("Music").as("item_category")
                 )
.show(false)

// COMMAND ----------

itemInfoDf.select($"*",
                 explode($"category"))
.withColumnRenamed("key","category_desc")
.withColumnRenamed("value","product_name")
.show

// COMMAND ----------

val allRowsWithValuesDf = webSalesDf.na.drop("all")

// COMMAND ----------

webSalesDf.count - allRowsWithValuesDf.count

// COMMAND ----------

display(res158)

// COMMAND ----------

val cleanDf = webSalesDf.na.drop("any")

// COMMAND ----------

val min2ValidColumnsDf  = itemDf.na.drop(4)

// COMMAND ----------

val cleanItemDf = itemDf.na.drop()

// COMMAND ----------

itemDf.na.drop("any",Seq("i_class","i_class_id"))

// COMMAND ----------

itemDf.na.drop("all",Seq("i_class","i_class_id"))

// COMMAND ----------

display(res161)

// COMMAND ----------

itemDf.na.fill("A Wonderful World").na.fill(9191919).na.fill(Map("i_item_id" -> "junior"))

// COMMAND ----------

