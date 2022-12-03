// Databricks notebook source
val incomeBandDf = spark.read
.schema("ib_lower_band_sk long, in_lower_bound int, ib_upper_bound int")
.option("sep", "|")
.csv("/FileStore/tables/income_band.dat")

// COMMAND ----------

incomeBandDf.show

// COMMAND ----------

val incomeBandwithGroups = incomeBandDf
.withColumn("isFirstIncomeGroup",incomeBandDf.col("ib_upper_bound") <= 60000)
.withColumn("isSecondIncomeGroup",incomeBandDf.col("ib_upper_bound") >= 60001 and incomeBandDf.col("ib_upper_bound") < 120001)
.withColumn("isThirdIncomeGroup",incomeBandDf.col("ib_upper_bound") >= 120001 and incomeBandDf.col("ib_upper_bound") <= 200000)
.withColumn("demo",lit)

// COMMAND ----------

incomeBandwithGroups.show

// COMMAND ----------

import org.apache.spark.sql.functions.lit

// COMMAND ----------

val incomeBandwithGroups = incomeBandDf
.withColumn("isFirstIncomeGroup",incomeBandDf.col("ib_upper_bound") <= 60000)
.withColumn("isSecondIncomeGroup",incomeBandDf.col("ib_upper_bound") >= 60001 and incomeBandDf.col("ib_upper_bound") < 120001)
.withColumn("isThirdIncomeGroup",incomeBandDf.col("ib_upper_bound") >= 120001 and incomeBandDf.col("ib_upper_bound") <= 200000)
.withColumn("demo",lit(1))

// COMMAND ----------

incomeBandwithGroups.show

// COMMAND ----------

incomeBandwithGroups.printSchema

// COMMAND ----------

val incomeClasses = incomeBandwithGroups
.withColumnRenamed("isThirdIncomeGroup","isHighIncomeClass")
.withColumnRenamed("isSecondIncomeGroup","isMediumIncomeClass")
.withColumnRenamed("isFirstIncomeGroup","isStandardIncomeClass")

// COMMAND ----------

incomeClasses.show

// COMMAND ----------

incomeClasses.drop("demo", "isHighIncomeClass","isStandardIncomeClass","isMediumIncomeClass")

// COMMAND ----------


