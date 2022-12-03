-- Databricks notebook source
create table if not exists retailer_db.catalog_sales
using csv
options(
path '/FileStore/tables/catalog_sales.dat',
sep '|',
header true
)

-- COMMAND ----------

create table if not exists retailer_db.item
using csv
options(
path '/FileStore/tables/retailer/data/item.dat',
sep '|',
header true
)

-- COMMAND ----------

create table if not exists retailer_db.datedim
using csv
options(
path '/FileStore/tables/retailer/data/date_dim.dat',
sep '|',
header true
)

-- COMMAND ----------

use retailer_db

-- COMMAND ----------

select 
it.i_manufact_id manufactId,
sum(cs_ext_discount_amt) excess_disc_amount
from catalog_sales cs
inner join item it on it.i_item_sk = cs.cs_item_sk
inner join datedim dt on dt.d_date_sk = cs.cs_sold_date_sk
--where it.i_manufact_id = 977
where dt.d_date between date("2000-01-27") and date_add("2000-01-27",90)
and cs.cs_ext_discount_amt > (select 1.3 * avg(cs_ext_discount_amt)
  from catalog_sales
  inner join datedim on d_date_sk = cs_sold_date_sk
  where cs_item_sk = it.i_item_sk
  and dt.d_date between date("2000-01-27") and date_add("2000-01-27",90)
 )
 group by it.i_manufact_id

-- COMMAND ----------

select avg(cs_ext_discount_amt) from catalog_sales