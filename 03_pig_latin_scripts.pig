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


/* Load emp.txt without schema */
-- Pig has a feature of lazy processing
-- It doesn't run until a command to dump is issued
-- It piles up all the commands until a dump is issued and runs all dependent commands in sequence
-- the dump command starts a map-reduce job and processes the relations as per users' commands
grunt> employee = load '/user/rajesh.kancharla_outlook/pig_files/emp.txt' using PigStorage(',');
grunt> dump employee;


/* Display the names from employee */
-- the employee dataset is created in the above step
-- from each entry of the relation, it picks up the second value ($1) and displays the results
-- the first column in the relation is represented with $0
grunt> name = foreach employee generate $1 ;
grunt> dump name;


/* Load emp.txt with schema */
-- While loading the data from a file, the structure of the data can be specified as a schema
grunt> employee = load '/user/rajesh.kancharla_outlook/pig_files/emp.txt' using PigStorage(',') as (emp_id:int, emp_name:chararray, emp_dept:chararray, emp_salary:int);
grunt> dump employee;

grunt> dept = load '/user/rajesh.kancharla_outlook/pig_files/dept.txt' using PigStorage(',') as (dept_id:chararray, dept_name:chararray);
grunt> dump dept;

/* Describe the relations */
describe employee;
describe dept;




