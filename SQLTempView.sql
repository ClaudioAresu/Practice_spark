-- Databricks notebook source
-- MAGIC %scala
-- MAGIC val customerAddress = spark.read
-- MAGIC .format("csv")
-- MAGIC .options(Map("header" -> "true",
-- MAGIC             "sep" -> "|",
-- MAGIC             "inferSchema" -> "true"))
-- MAGIC .load("/FileStore/tables/retailer/data/customer_address.dat")

-- COMMAND ----------

-- MAGIC %scala
-- MAGIC customerAddress.show

-- COMMAND ----------

-- MAGIC %scala
-- MAGIC customerAddress.createTempView("vCustomerAddress")

-- COMMAND ----------

-- MAGIC 
-- MAGIC %scala
-- MAGIC //same as customerAddress.select("*")
-- MAGIC spark.sql("select  * from vCustomerAddress").show

-- COMMAND ----------

-- MAGIC %scala
-- MAGIC customerAddress
-- MAGIC .select("ca_address_sk","ca_country","ca_city","ca_street_name")
-- MAGIC .where($"ca_state".contains("AK"))
-- MAGIC .createTempView("vAK_Addresses")

-- COMMAND ----------

select * from vAK_Addresses

-- COMMAND ----------

select * from vCustomerAddress

-- COMMAND ----------

-- MAGIC %scala
-- MAGIC val customerDf = spark.read
-- MAGIC .format("csv")
-- MAGIC .options(Map("header" -> "true",
-- MAGIC             "sep" -> "|",
-- MAGIC             "inferSchema" -> "true"))
-- MAGIC .load("/FileStore/tables/retailer/data/customer.dat")

-- COMMAND ----------

-- MAGIC %scala
-- MAGIC customerDf.createGlobalTempView("gvCustomer")

-- COMMAND ----------

select * from global_temp.gvCustomer

-- COMMAND ----------

create table if not exists demotbl as select * from vCustomerAddress

-- COMMAND ----------

create database rettailer_db

-- COMMAND ----------

drop database rettailer_db

-- COMMAND ----------

create database retailer_db

-- COMMAND ----------

show databases

-- COMMAND ----------

select * from demotbl limit 10

-- COMMAND ----------

select current_database()

-- COMMAND ----------

use retailer_db

-- COMMAND ----------

show tables

-- COMMAND ----------

use default

-- COMMAND ----------

show tables

-- COMMAND ----------

create database dbname

-- COMMAND ----------

show databases

-- COMMAND ----------

drop database dbname

-- COMMAND ----------

select current_database()

-- COMMAND ----------

create table if not exists retailer_db.customer(c_customer_sk long comment "this is the primary key",c_customer_id string,c_current_cdemo_sk long,c_current_hdemo_sk long,
c_current_addr_sk long,c_first_shipto_date_sk long,c_first_sales_date_sk long,c_salutation string,c_first_name string,
c_last_name string,c_preferred_cust_flag string,c_birth_day int,c_birth_month int,c_birth_year int,c_borth_country string,
c_login string,c_email_address string,c_last_review_date long)
using csv
options(
path '/FileStore/tables/retailer/data/customer.dat',
sep '|',
header true
)

-- COMMAND ----------

show tables in retailer_db

-- COMMAND ----------

describe formatted retailer_db.customer

-- COMMAND ----------

-- MAGIC %fs ls 
-- MAGIC dbfs:/FileStore/tables/retailer/data/customer.dat

-- COMMAND ----------

select current_database()

-- COMMAND ----------

drop table retailer_db.customer

-- COMMAND ----------

show tables in retailer_db

-- COMMAND ----------

drop table  retailer_db.customerAddress

-- COMMAND ----------

create table retailer_db.customerWithAddress(customer_id bigint, first_name string,last_name string, city string, state string, street_name string)

-- COMMAND ----------

drop table customer

-- COMMAND ----------

create table if not exists retailer_db.customerAddress(ca_address_sk long,ca_address_id string,ca_street_number string,ca_street_name string,ca_street_type string,ca_suite_number string,ca_city string,ca_county string,ca_state string,ca_zip int,ca_country string,ca_gmt_offset int,ca_location_type string)
using csv
options(
path '/FileStore/tables/retailer/data/customer_address.dat',
sep '|',
header true
)

-- COMMAND ----------

show tables in retailer_db

-- COMMAND ----------

describe retailer_db.customeraddress

-- COMMAND ----------

use retailer_db

-- COMMAND ----------

create table retailer_db.customerWithAddress(customer_id bigint, first_name string,last_name string, country string, city string, state string, street_name string)

-- COMMAND ----------

insert into retailer_db.customerwithaddress
select c_customer_sk, c_first_name, c_last_name, ca_country, ca_city, ca_state, ca_street_name
from retailer_db.customer c
inner join customeraddress ca on ca.ca_address_sk = c.c_current_addr_sk


-- COMMAND ----------

show tables in retailer_db

-- COMMAND ----------

select * from retailer_db.customerwithaddress

-- COMMAND ----------

describe formatted retailer_db.customerwithaddress

-- COMMAND ----------

describe formatted customer

-- COMMAND ----------

-- MAGIC %fs ls dbfs:/user/hive/warehouse/retailer_db.db/customerwithaddress

-- COMMAND ----------

drop table retailer_db.customeraddress

-- COMMAND ----------

use retailer_db

-- COMMAND ----------

describe customer

-- COMMAND ----------

select 
c_customer_sk,
c_customer_sk * 2,
c_salutation,
upper(c_first_name),
c_first_name, 
c_last_name
from customer

-- COMMAND ----------

select 2 * 2

-- COMMAND ----------

-- MAGIC %scala
-- MAGIC import org.apache.spark.sql.functions.{expr, col}

-- COMMAND ----------

-- MAGIC %scala
-- MAGIC customerAddress.select(col("ca_address_sk").as("id1"), expr("ca_address_sk as id")).show

-- COMMAND ----------

-- MAGIC %scala
-- MAGIC customerAddress.selectExpr("ca_address_sk",
-- MAGIC                           "upper(ca_street_name)",
-- MAGIC                            "ca_street_number",
-- MAGIC                            "ca_street_number is not null and length(ca_street_number) > 0 as isValidAddress").show

-- COMMAND ----------

-- MAGIC %scala
-- MAGIC val validAddress = customerAddress.select(
-- MAGIC $"ca_address_sk",
-- MAGIC $"ca_street_name",
-- MAGIC $"ca_street_number",
-- MAGIC expr("ca_street_number is not null and length(ca_street_number) > 0 as isValidAddress")).show

-- COMMAND ----------

select * from customer

-- COMMAND ----------

create table if not exists retailer_db.customer(c_customer_sk long comment "this is the primary key",c_customer_id string,c_current_cdemo_sk long,c_current_hdemo_sk long,
c_current_addr_sk long,c_first_shipto_date_sk long,c_first_sales_date_sk long,c_salutation string,c_first_name string,
c_last_name string,c_preferred_cust_flag string,c_birth_day int,c_birth_month int,c_birth_year int,c_birth_country string,
c_login string,c_email_address string,c_last_review_date long)
using csv
options(
path '/FileStore/tables/retailer/data/customer.dat',
sep '|',
header true
)

-- COMMAND ----------

select length(c_salutation) from customer

-- COMMAND ----------

select * from customer 
where c_birth_month = 1
and lower(c_birth_country) = "canada"
and c_birth_year <> 1980
and trim(c_salutation) = "Miss" or trim(c_salutation) = "Mrs."

-- COMMAND ----------

select * from customer
where c_birth_country is not null

-- COMMAND ----------

select count(*) from customer

-- COMMAND ----------

select count(c_first_name) 
from customer

-- COMMAND ----------

select count(*)
from customer 
where c_first_name is null

-- COMMAND ----------

create table if not exists retailer_db.store_sales
using csv
options(
path '/FileStore/tables/store_sales.dat',
sep '|',
header true,
inferSchema true
)

-- COMMAND ----------

select sum(ss_quantity) from store_sales

-- COMMAND ----------

select sum(ss_net_profit) from store_sales

-- COMMAND ----------

select avg(ss_net_paid) from store_sales

-- COMMAND ----------

select min(ss_net_paid) from store_sales


-- COMMAND ----------

select max(ss_net_paid) from store_sales


-- COMMAND ----------

select * from store_sales where ss_net_paid = 0

-- COMMAND ----------

select * from store_sales where ss_net_paid = 19562.4 

-- COMMAND ----------

select ss_store_sk,
sum(ss_net_profit)
from store_sales
where ss_store_sk is not null
group by ss_store_sk


-- COMMAND ----------

select sum(ss_net_profit) from store_sales where ss_store_sk = 1

-- COMMAND ----------

select ss_store_sk,
ss_item_sk,
count(ss_quantity),
sum(ss_net_profit) as ss_total_net
from store_sales
where ss_store_sk is not null
and ss_item_sk is not null
group by ss_store_sk, ss_item_sk
having sum(ss_net_profit)> 0
order by ss_total_net
-- order by ss_store_sk desc

-- COMMAND ----------

select count(*) from customer where c_birth_year = 1943 and c_birth_country = "GREECE"

-- COMMAND ----------

select c_birth_year, 
c_birth_country, 
count(*) as c_count
from customer
where c_birth_country is not null
and c_birth_year is not null
group by c_birth_year, c_birth_country
having c_birth_country like "A%" and count(*) > 10

-- COMMAND ----------

 select c_birth_year, 
c_birth_country, 
count(*) as c_count
from customer
where c_birth_country is not null
and c_birth_year is not null
group by c_birth_year, c_birth_country
having max(c_birth_month) = 2
order by c_count desc, c_birth_country

-- COMMAND ----------

select distinct c_birth_month from customer where c_birth_year = 1941 and c_birth_country = "DENMARK"

-- COMMAND ----------

select * from customer where c_birth_year = 1982 and c_birth_country = "MALDIVES"

-- COMMAND ----------

create table if not exists retailer_db.customerAddress(ca_address_sk long,ca_address_id string,ca_street_number string,ca_street_name string,ca_street_type string,ca_suite_number string,ca_city string,ca_county string,ca_state string,ca_zip int,ca_country string,ca_gmt_offset int,ca_location_type string)
using csv
options(
path '/FileStore/tables/retailer/data/customer_address.dat',
sep '|',
header true
)

-- COMMAND ----------

select * from retailer_db.customer c
inner join retailer_db.customerAddress ca
on c.c_current_addr_sk = ca.ca_address_sk

-- COMMAND ----------

select
--c_customer_sk,
--c_customer_sk,
--c_first_name,
--c_last_name,
ca_city,
count(*)
--ca_state,
--ca_street_name,
--ca_street_number,
--ca_county
from retailer_db.customer c
inner join retailer_db.customerAddress ca
on c.c_current_addr_sk = ca.ca_address_sk
where ca_city is not null
group by ca_city order by count(*) desc

-- COMMAND ----------

select * from customer c
left outer join  store_sales s
on c.c_customer_sk = s.ss_customer_sk

-- COMMAND ----------

select count(distinct ss_customer_sk) from store_sales

-- COMMAND ----------

select count(distinct c_customer_sk) from customer

-- COMMAND ----------

select

--c.c_customer_sk customerId,
--concat(c.c_first_name,c.c_last_name),
--s.ss_customer_sk customerId_from_store,
--s.ss_item_sk itemId
from customer c
left outer join  store_sales s
on c.c_customer_sk = s.ss_customer_sk
where s.ss_customer_sk 
is null

-- COMMAND ----------

select 100000 - 
90858

-- COMMAND ----------

select
--count(*)
c.c_customer_sk customerId,
concat(c.c_first_name,c.c_last_name),
s.ss_customer_sk customerId_from_store,
s.ss_store_sk,
s.ss_ticket_number,
s.ss_item_sk itemId
from customer c
right outer join  store_sales s
on c.c_customer_sk = s.ss_customer_sk
--where c.c_first_name is not null and c.c_last_name is not null
--and
where s.ss_customer_sk is null

-- COMMAND ----------

select
c_customer_sk,
c_first_name,
c_last_name,
c_birth_year,
c_birth_country
from customer
where c_birth_country in ("SURINAME","FIJI","TOGO")
and c_birth_year between 1966 and 1980
and c_last_name like "M%"
and c_first_name like "_e%"

-- COMMAND ----------

select
ss_item_sk,
ss_sales_price,
ss_customer_sk,
case
when ss_sales_price between 0 and 37.892353 then"belowAvg"
when ss_sales_price between 37.892353 and 199.56 then"avgOrAbove"
else "unknown"
end as priceCategory
from store_sales

-- COMMAND ----------

  