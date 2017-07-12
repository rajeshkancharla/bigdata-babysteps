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
-- describes the structure of the relation
describe employee;
describe dept;


/* Filter data */
-- Filters the data by a condition or a combination of conditions
grunt> moresalary = FILTER employee BY emp_salary > 2000;
grunt> dump moresalary;

(5,Steve,IT,2500)
(6,Brian,IT,2700)
(7,Michael,HR,3000)
(8,Steve,SALES,10000)
(9,Peter,HR,7000)
(10,Dan,IT,6000)

grunt> midsalary = FILTER employee BY emp_salary > 2000 AND emp_salary < 6000;
grunt> dump midsalary;

(5,Steve,IT,2500)
(6,Brian,IT,2700)
(7,Michael,HR,3000)


/* Sort Data */
-- Sorts data by a column or a group of columns in the desired order
grunt> orderemp = ORDER employee BY emp_name ASC;
grunt> dump orderemp;

(4,Adam,IT,2000)
(6,Brian,IT,2700)
(10,Dan,IT,6000)
(3,Henry,HR,1500)
(1,Mark,HR,1000)
(7,Michael,HR,3000)
(9,Peter,HR,7000)
(2,Peter,SALES,1200)
(8,Steve,SALES,10000)
(5,Steve,IT,2500)


/* Group Data */
-- Groups data by a column
-- The relation created has the full structure based on the position of columns in the output
grunt> groupemp = GROUP employee BY emp_dept;
grunt> dump groupemp;

(HR,{(9,Peter,HR,7000),(7,Michael,HR,3000),(3,Henry,HR,1500),(1,Mark,HR,1000)})
(IT,{(10,Dan,IT,6000),(6,Brian,IT,2700),(5,Steve,IT,2500),(4,Adam,IT,2000)})
(SALES,{(8,Steve,SALES,10000),(2,Peter,SALES,1200)})

grunt> describe groupemp;
groupemp: {group: chararray,employee: {(emp_id: int,emp_name: chararray,emp_dept: chararray,emp_salary: int)}}

-- Relations can be created from relations as well
grunt> A = foreach groupemp generate group;
grunt> dump A;

(HR)
(IT)
(SALES)


/* Select Columns */
-- We can select one more more columns in a relation
grunt> name_sal = FOREACH employee GENERATE emp_name, emp_salary;
grunt> dump name_sal;

(Mark,1000)
(Peter,1200)
(Henry,1500)
(Adam,2000)
(Steve,2500)
(Brian,2700)
(Michael,3000)
(Steve,10000)
(Peter,7000)
(Dan,6000)


/* Split data */
-- Splits the relation into multiple relations based on a condition or combination of conditions
grunt> split employee into A if emp_salary < 5000, B if emp_salary > 5000;

grunt> dump A;

(1,Mark,HR,1000)
(2,Peter,SALES,1200)
(3,Henry,HR,1500)
(4,Adam,IT,2000)
(5,Steve,IT,2500)
(6,Brian,IT,2700)
(7,Michael,HR,3000)

grunt> dump B;

(8,Steve,SALES,10000)
(9,Peter,HR,7000)
(10,Dan,IT,6000)


/* Join Data */
-- Joins one or more relations by the column present in each of the relations
grunt> empdept = JOIN employee by emp_dept, dept by dept_id;

grunt> dump empdept;

(9,Peter,HR,7000,HR,Human Resource)
(7,Michael,HR,3000,HR,Human Resource)
(3,Henry,HR,1500,HR,Human Resource)
(1,Mark,HR,1000,HR,Human Resource)
(10,Dan,IT,6000,IT,Information Technology)
(6,Brian,IT,2700,IT,Information Technology)
(5,Steve,IT,2500,IT,Information Technology)
(4,Adam,IT,2000,IT,Information Technology)
(8,Steve,SALES,10000,SALES, Sales Department)
(2,Peter,SALES,1200,SALES, Sales Department)

grunt> describe empdept;
empdept: {employee::emp_id: int,employee::emp_name: chararray,employee::emp_dept: chararray,employee::emp_salary: int,dept::dept_id: chararray,dept::dept_name: chararray}

grunt> name_sal = FOREACH employee GENERATE emp_name, emp_salary;
grunt> dump name_sal;

(Peter,7000)
(Michael,3000)
(Henry,1500)
(Mark,1000)
(Dan,6000)
(Brian,2700)
(Steve,2500)
(Adam,2000)
(Steve,10000)
(Peter,1200)

grunt> describe name_sal;
name_sal: {employee::emp_name: chararray,employee::emp_salary: int}


/* Combination of commands */
-- Full name of department where salary of employee > 6000:
empdept1 = JOIN employee by emp_dept, dept by dept_id;
salfilter = FILTER empdept1 BY emp_salary > 6000;
dept_name1 = FOREACH salfilter GENERATE dept_name;
dump dept_name1;

(Human Resource)
(Sales Department)


/* Word Count Program */
-- There is a file in the HDFS
-- It is required to count the number of words in the file
A = load '/user/rajesh.kancharla_outlook/pig_files/emp.txt';
B = FOREACH A GENERATE FLATTEN(TOKENIZE($0)) as word;
C = GROUP B by word;
D = FOREACH C GENERATE group, COUNT(B);
dump D;

