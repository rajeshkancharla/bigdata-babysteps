-- Pig Latin is a dataflow language and environment for exploring very large datasets
-- It lets you specify a sequence of data transformations such as merging data sets, filtering them, and applying functions to records or groups of records. 
-- Pig comes with many built-in functions but you can also create your own user-defined functions to do special-purpose processing. 
-- Pig Latin programs run in a distributed fashion on a cluster (programs are complied into Map/Reduce jobs and executed using Hadoop). 
-- For quick prototyping, Pig Latin programs can also run in "local mode" without a cluster (all processing takes place in a single local JVM).

-- For the purpose of understanding Pig Latin at a very basic level, the following files are used.

/* Source Files

emp.txt file:
-------------
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


dept.txt file:
--------------
HR,Human Resource
SALES, Sales Department
IT,Information Technology

*/


-- Step-1: Load emp.txt without schema
-- Pig has a feature of lazy processing
-- It doesn't run until a command to dump is issued
-- It piles up all the 
grunt> employee = load '/user/rajesh.kancharla_outlook/pig_files/emp.txt' using PigStorage(',');
grunt> dump employee;
