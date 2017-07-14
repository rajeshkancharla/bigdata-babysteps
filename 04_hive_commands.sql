/*
Hive is a data warehousing infrastructure based on Apache Hadoop. Hive is designed to enable easy data summarization, ad-hoc querying and analysis of large volumes of data. It provides SQL which enables users to do ad-hoc querying, summarization and data analysis easily. Hive is not designed for Online Transaction processing. It is best used for traditional data warehousing tasks
*/

/* Source files */
-- emp.txt
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



