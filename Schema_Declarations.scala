// Databricks notebook source
val customerSchemaDDL = "c_customer_sk long,c_customer_id string,c_current_cdemo_sk long,c_current_hdemo_sk long,"+
"c_current_addr_sk long,c_first_shipto_date_sk long,c_first_sales_date_sk long,c_salutation string,c_first_name string,"+
"c_last_name string,c_preferred_cust_flag string,c_birth_day int,c_birth_month int,c_birth_year int,c_borth_country string,"+
"c_login string,c_email_address string,c_last_review_date long"

// COMMAND ----------

val customerAddressSchema = "ca_address_sk long,ca_address_id string,ca_street_number string,ca_street_name string,ca_street_type string,"+"ca_suite_number string,ca_city string,ca_county string,ca_state string,"+"ca_zip int,ca_country string,ca_gmt_offset int,"+"ca_location_type string"

// COMMAND ----------

val HouseHoldDemoSchema = "hd_demo_sk long,hd_income_band_sk long,hd_buy_potential string,hd_dep_count long,"+"hd_vehicle_count int"

// COMMAND ----------

val IncomeBandSchema = "ib_income_band_sk long,ib_lower_bound long,ib_upper_bound long"