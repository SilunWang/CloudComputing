-- Write down the SQL statements you wrote to createing optimized tables
-- and to populate those tables in this file.
-- Remember to add comments explaining why you did so.

-- start impala_create_table_optimized
-- use PARQUET to speed up column aggregation
-- Q2 examines p_category, partition
CREATE EXTERNAL TABLE part_opt (
    p_partkey int,
    p_name STRING, 
    p_mfgr STRING, 
    p_brand1 STRING, 
    p_color STRING, 
    p_type STRING, 
    p_size INT, 
    p_container STRING)
partitioned by (p_category STRING)
STORED AS PARQUET;
INSERT into table part_opt partition (p_category) 
select 
    p_partkey,
    p_name, 
    p_mfgr, 
    p_brand1, 
    p_color, 
    p_type, 
    p_size, 
    p_container,
    p_category
from part;
-- Q2 examines s_region, partition
CREATE EXTERNAL TABLE supplier_opt (
    s_suppkey   INT,
    s_name STRING,
    s_address STRING,
    s_city STRING,
    s_nation STRING,
    s_phone STRING)
partitioned by (s_region STRING)
STORED AS PARQUET;
INSERT into table supplier_opt partition (s_region)
select 
    s_suppkey,
    s_name,
    s_address,
    s_city,
    s_nation,
    s_phone,
    s_region
from supplier;

-- Q3 examines c_city, partition
CREATE EXTERNAL TABLE customer_opt (
    c_custkey int,
    c_name STRING,
    c_address  STRING,
    c_nation STRING,
    c_region STRING,
    c_phone STRING,
    c_mktsegment STRING)
partitioned by (c_city STRING)
STORED AS PARQUET;
INSERT into table customer_opt partition (c_city)
select 
    c_custkey,
    c_name,
    c_address ,
    c_nation,
    c_region,
    c_phone,
    c_mktsegment, 
    c_city
from customer;

CREATE EXTERNAL TABLE dwdate_opt (
    d_datekey int,
    d_date STRING,
    d_dayofweek STRING,
    d_month STRING,
    d_year INT,
    d_yearmonthnum INT,
    d_yearmonth STRING,
    d_daynuminweek INT,
    d_daynuminmonth INT,
    d_daynuminyear INT,
    d_monthnuminyear INT,
    d_weeknuminyear INT,
    d_sellingseason STRING,
    d_lastdayinweekfl STRING,
    d_lastdayinmonthfl STRING,
    d_holidayfl STRING,
    d_weekdayfl STRING)
STORED AS PARQUET;
INSERT into table dwdate_opt 
select *
from dwdate;

-- Q1 examines lo_discount
CREATE EXTERNAL TABLE lineorder_opt2 (
    lo_orderkey INT,
    lo_linenumber INT,
    lo_custkey INT,
    lo_partkey INT,
    lo_suppkey INT,
    lo_orderdate INT,
    lo_orderpriority STRING,
    lo_shippriority STRING,
    lo_quantity INT,
    lo_extendedprice INT,
    lo_ordertotalprice INT,
    lo_revenue INT,
    lo_supplycost INT,
    lo_tax INT,
    lo_commitdate INT,
    lo_shipmode STRING)
partitioned by (lo_discount INT)
STORED AS PARQUET;
INSERT into table lineorder_opt2 partition (lo_discount)
select 
    lo_orderkey,
    lo_linenumber,
    lo_custkey,
    lo_partkey,
    lo_suppkey,
    lo_orderdate,
    lo_orderpriority,
    lo_shippriority,
    lo_quantity,
    lo_extendedprice,
    lo_ordertotalprice,
    lo_revenue,
    lo_supplycost,
    lo_tax,
    lo_commitdate,
    lo_shipmode,
    lo_discount
from lineorder;

-- Prepare a non-partitioned lineorder table
CREATE EXTERNAL TABLE lineorder_opt3 (
    lo_orderkey INT,
    lo_linenumber INT,
    lo_custkey INT,
    lo_partkey INT,
    lo_suppkey INT,
    lo_orderdate INT,
    lo_orderpriority STRING,
    lo_shippriority STRING,
    lo_quantity INT,
    lo_extendedprice INT,
    lo_ordertotalprice INT,
    lo_discount INT,
    lo_revenue INT,
    lo_supplycost INT,
    lo_tax INT,
    lo_commitdate INT,
    lo_shipmode STRING)
STORED AS PARQUET;
INSERT into table lineorder_opt3
select *
from lineorder;
-- end impala_create_table_optimized

-- start query1_opt
-- put big tables on the left
-- make join operation more efficient
select sum(product) as revenue from 
(select lo_extendedprice*lo_discount as product, lo_orderdate from lineorder_opt2 where lo_discount between 1 and 3 and lo_quantity < 24) as t1,
(select d_datekey from dwdate_opt where d_year=1997) as t2
where t1.lo_orderdate=t2.d_datekey;
-- end query1_opt

-- start query2_opt
-- put big tables on the left
-- use partitioned lineorder_opt2 will be slow
select sum(lo_revenue), d_year, p_brand1 from lineorder_opt3, dwdate_opt, part_opt, supplier_opt
where s_region = 'AMERICA' 
and p_category = 'MFGR#12' 
and lo_orderdate = d_datekey 
and lo_partkey = p_partkey 
and lo_suppkey = s_suppkey
group by d_year, p_brand1 order by d_year, p_brand1 limit 500;
-- end query2_opt

-- start query3_opt
-- put big tables on the left
select c_city, s_city, d_year, sum(lo_revenue) as revenue from 
(select lo_custkey, lo_orderdate, lo_revenue, lo_suppkey from lineorder_opt2) as t4,
(select d_datekey, d_year from dwdate_opt where d_yearmonth = 'Dec1997') as t3,
(select c_custkey, c_city from customer_opt where c_city='UNITED KI1' or c_city='UNITED KI5') as t1,
(select s_suppkey, s_city from supplier_opt where s_city='UNITED KI1' or s_city='UNITED KI5') as t2
where t4.lo_custkey = t1.c_custkey and t4.lo_suppkey = t2.s_suppkey and t4.lo_orderdate = t3.d_datekey 
group by c_city, s_city, d_year order by d_year asc, revenue desc limit 5;
-- end query3_opt

-- start redshift_create_table_optimized
CREATE TABLE part (
  p_partkey         integer         not null sortkey,
  p_name            varchar(22)     not null,
  p_mfgr            varchar(6)      not null,
  p_category        varchar(7)      not null,
  p_brand1          varchar(9)      not null,
  p_color           varchar(11)     not null,
  p_type            varchar(25)     not null,
  p_size            integer         not null,
  p_container       varchar(10)     not null
) diststyle all;
CREATE TABLE supplier (
  s_suppkey         integer        not null sortkey,
  s_name            varchar(25)    not null,
  s_address         varchar(25)    not null,
  s_city            varchar(10)    not null,
  s_nation          varchar(15)    not null,
  s_region          varchar(12)    not null,
  s_phone           varchar(15)    not null
) diststyle all;
CREATE TABLE customer (
  c_custkey         integer        not null sortkey distkey,
  c_name            varchar(25)    not null,
  c_address         varchar(25)    not null,
  c_city            varchar(10)    not null,
  c_nation          varchar(15)    not null,
  c_region          varchar(12)    not null,
  c_phone           varchar(15)    not null,
  c_mktsegment      varchar(10)    not null
);
CREATE TABLE dwdate (
  d_datekey            integer       not null sortkey,
  d_date               varchar(19)   not null,
  d_dayofweek         varchar(10)   not null,
  d_month           varchar(10)   not null,
  d_year               integer       not null,
  d_yearmonthnum       integer       not null,
  d_yearmonth          varchar(8)   not null,
  d_daynuminweek       integer       not null,
  d_daynuminmonth      integer       not null,
  d_daynuminyear       integer       not null,
  d_monthnuminyear     integer       not null,
  d_weeknuminyear      integer       not null,
  d_sellingseason      varchar(13)    not null,
  d_lastdayinweekfl    varchar(1)    not null,
  d_lastdayinmonthfl   varchar(1)    not null,
  d_holidayfl          varchar(1)    not null,
  d_weekdayfl          varchar(1)    not null
) diststyle all;
CREATE TABLE lineorder (
  lo_orderkey       integer         not null,
  lo_linenumber         integer         not null,
  lo_custkey            integer         not null distkey,
  lo_partkey            integer         not null,
  lo_suppkey            integer         not null,
  lo_orderdate          integer         not null sortkey,
  lo_orderpriority      varchar(15)     not null,
  lo_shippriority       varchar(1)      not null,
  lo_quantity           integer         not null,
  lo_extendedprice      integer         not null,
  lo_ordertotalprice    integer         not null,
  lo_discount           integer         not null,
  lo_revenue            integer         not null,
  lo_supplycost         integer         not null,
  lo_tax                integer         not null,
  lo_commitdate         integer         not null,
  lo_shipmode           varchar(10)     not null
);
-- end redshift_create_table_optimized