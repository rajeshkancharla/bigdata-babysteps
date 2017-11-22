/*
Hive is a data warehousing infrastructure based on Apache Hadoop. Hive is designed to enable easy data summarization, ad-hoc querying and analysis of large volumes of data. It provides SQL which enables users to do ad-hoc querying, summarization and data analysis easily. Hive is not designed for Online Transaction processing. It is best used for traditional data warehousing tasks
*/

/* Source files */
-- emp.txt
/*
1,Mark,HR,1000
2,Peter,SALES,1200
3,Henry,HR,1500
4,Adam,IT,2000
5,Steve,IT,2500
6,Brian,IT,2700
7,Michael,HR,3000
8,Steve,SALES,10000
9,Peter,HR,7000
10,Dan,IT,6000

-- dept.txt
HR,Human Resource
SALES,Sales Department
IT,Information Technology

-- external table source data
1,James,aa,m,us,oh,1,2015,8,10
2,Nick,bb,m,us,ca,1,2015,8,11
3,Sara,aa,f,us,fl,3,2015,8,15
4,Michel,cc,m,us,oh,1,2015,8,20
5,Linc,aa,m,us,ca,1,2015,7,10
6,Juan,bb,m,us,il,1,2015,7,12
7,Kitty,cc,f,us,oh,1,2015,7,17
8,Mathew,aa,m,us,ca,1,2015,7,18
9,Ricky,dd,m,us,fl,7,2015,6,4
10,Brett,aa,m,us,il,1,2015,6,11
*/

/* Launch Hive */
-- When there is a setup available for HDFS, use the command hive or any other command provided by the service provider
-- This command should launch hive command shell which is ready for acceptiing inputs
hive

/* Create Database */
-- This statement creates a folder by name rajeshk.db in the hive home directory - /apps/hive/warehouse.
-- Folder name with database: /apps/hive/warehouse/rajeshk.db
hive> CREATE DATABASE rajeshk;

-- start using the database rajeshk
hive> USE rajeshk;

/* Create Managed / Internal Table */
-- This statement creates a folder with the name as table name. 
-- However there will be no files under this folder because data is not there yet.
hive> create table employee (e_id int, e_name string, e_dept string, e_salary int) ROW FORMAT DELIMITED FIELDS TERMINATED BY ',';

/* Load Data from HDFS directory */
-- While using the managed table, the data gets moved from the source file path to hive.
-- The source file gets removed as it's virtually moved from source path to hive path
-- In the below example, the hive_files is a directory in the HDFS which contains the file emp.txt
hive> load data inpath 'hive_files/emp.txt' into table employee;

/* Load Data from Local directory */
-- The source file can exist in local directory also in the file system
-- When the file from local directory is loaded, it will not be removed and will continue to stay in local directory
-- Virtually, a copy of the local file is created in HDFS's hive warehouse
hive> create table emp_local (e_id int, e_name string, e_dept string, e_salary int) ROW FORMAT DELIMITED FIELDS TERMINATED BY ',';
hive> load data local inpath '/home/rajesh.kancharla_outlook/pig/emp.txt' into table emp_local;

/* Joins */
-- Create a table for dept and load it
create table dept (d_id string, d_name string) ROW FORMAT DELIMITED FIELDS TERMINATED BY ',';
load data local inpath '/home/rajesh.kancharla_outlook/pig/dept.txt' into table dept;

-- Join the two tables employee and dept
-- the below command runs a map-reduce job and returns the output.
hive> select e_name, d_name
    > from employee, dept
    > where e_dept = d_id;

-- another way to join
hive> select e_name, d_name from employee JOIN dept on e_dept = d_id;

-- Sort the data
-- This command triggers a map-reduce job and returns the data in sorted order.
-- when there is no filter, it returns results immediately but if there are any conditions, it triggers a map-reduce job.
hive> select * from employee order by e_dept;

/* Internal vs External */
-- Internal tables are linked to the file in the HDFS. When the table is dropped, the metadata of the table and the data are gone
-- External tables are not linked to the file. Even if table is dropped, the data still remains.

/* External Table */
create external table ex_test (user_id int, common_name string, user_type string, gender string, country string, state string, freq string, year int, month int, day int) 
ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' 
LOCATION '/user/rajesh.kancharla_outlook/hive_files/retention';

SELECT * FROM ex_test
-- The above command goes into the folder on which the ex_test is built on.
-- It concatenates all the files in the folder and shows the results
-- If the files in the path are not of the same structure, wherever the data type matches, the data gets loaded and others become NULL
-- So it must be ensured of consistent format while using files for external table in hive

-- *************************************************************************************************************************************
/* PARTITIONING */

-- Partitioning is used for speeding up the search activities and logical segregation of the data
-- There are Static and Dynamic partitioning in Hive
-- When the data is already segregated based on certain parameter, then static partitioning can be used

/* STATIC PARTITIONING */
-- Create partitioned table
hive> create table emp_spart(e_id int, e_name string, e_salary int) partitioned by (e_dept string)
row format delimited fields terminated by ',';

-- Load data into partitioned table
-- Note that the input data is already partitioned by the partition key in different files
-- So all the files are loaded separately into different partitions
hive> load data local inpath '/home/rajesh.kancharla_outlook/pig/emp_hr_1.txt' into table emp_spart partition(e_dept = 'HR');
hive> load data local inpath '/home/rajesh.kancharla_outlook/pig/emp_it_1.txt' into table emp_spart partition(e_dept = 'IT');
hive> load data local inpath '/home/rajesh.kancharla_outlook/pig/emp_sales_1.txt' into table emp_spart partition(e_dept = 'SALES');

-- the partitions are created as separate directories with the name as partition
-- Ex: /apps/hive/warehouse/rajeshk.db/emp_spart/e_dept=HR
--     /apps/hive/warehouse/rajeshk.db/emp_spart/e_dept=IT
--     /apps/hive/warehouse/rajeshk.db/emp_spart/e_dept=SALES

-- to see all the partitions on a table
hive> show partitions emp_spart;


/* DYNAMIC PARTITIONING */

-- We always need an external staging table while using the dynamic partitioning
-- The location pointed here is from HDFS in which there is one single file emp.txt
-- There are no separate files split by the partitioned key dept.
hive> create external table emp_stage (e_id int, e_name string, e_dept string, e_salary int) 
ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
LOCATION '/user/rajesh.kancharla_outlook/ex_data';

-- create the dynamic partitioned table
hive> create table emp_dpart (e_id int, e_name string, e_salary int) partitioned by (e_dept string) 

-- Parameters required for dynamic partitioning
-- these are session level parameters but these can be set forever by admin
-- there is no impact on static partitioning when they are enabled for dynamic partitioning
hive> SET hive.exec.dynamic.partition=true;
hive> SET hive.exec.dynamic.partition.mode=nonstrict;

-- Insert data into dynamic partitioned table from the staging table
hive> insert into table emp_dpart partition(e_dept) select e_id, e_name, e_salary, e_dept from emp_stage;

-- the partitions are created as separate directories with the name as partition
-- Ex: /apps/hive/warehouse/rajeshk.db/emp_dpart/e_dept=HR
--     /apps/hive/warehouse/rajeshk.db/emp_dpart/e_dept=IT
--     /apps/hive/warehouse/rajeshk.db/emp_dpart/e_dept=SALES

-- to see all the partitions on a table
hive> show partitions emp_dpart;


-- *************************************************************************************************************************************
/* BUCKETING */
-- The data is distributed into buckets
-- The number of buckets is predefined by the user
-- The basis for routing data into buckets is determined by (ID Value of the column) % (Number of Buckets)
-- Hashing function takes care of distribution of data

-- Set the environment variable to enable bucketing
hive> set hive.enforce.bucketing=true

-- An external staging table is required for loading data into bucketed table
hive> create external table emp_stage (e_id int, e_name string, e_dept string, e_salary int) 
ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
LOCATION '/user/rajesh.kancharla_outlook/ex_data';

-- Create Bucketed table
hive> create table b_emp(e_id int, e_name string, e_dept string, e_salary int) 
clustered by (e_id) into 4 buckets
row format delimited fields terminated by ',';

-- Insert data into bucketed table
insert into table b_emp select * from emp_stage;

-- At this stage, a directory b_emp is created in hive warehouse with 4 files as there are 4 buckets
-- Ex: /apps/hive/warehouse/rajeshk.db/b_emp/000000_0.deflate
--     /apps/hive/warehouse/rajeshk.db/b_emp/000001_0.deflate
--     /apps/hive/warehouse/rajeshk.db/b_emp/000002_0.deflate
--     /apps/hive/warehouse/rajeshk.db/b_emp/000003_0.deflate
-- All the files are in binary format, however in order to read the contents of each of the bucketed files, use

hive> select * from b_emp tablesample(bucket 1 out of 4 on e_id);
hive> select * from b_emp tablesample(bucket 2 out of 4 on e_id);
hive> select * from b_emp tablesample(bucket 3 out of 4 on e_id);
hive> select * from b_emp tablesample(bucket 4 out of 4 on e_id);


-- In order to check the file system from hive prompt use dfs command
hive> dfs -ls <path_name>



