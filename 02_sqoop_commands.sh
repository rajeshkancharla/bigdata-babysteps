# About Sqoop:
# ============
# Sqoop is a tool designed to transfer data between Hadoop and relational databases or mainframes. You can use Sqoop to import data from a relational database management system (RDBMS) such as MySQL or Oracle or a mainframe into the Hadoop Distributed File System (HDFS), transform the data in Hadoop MapReduce, and then export the data back into an RDBMS.
# Sqoop automates most of this process, relying on the database to describe the schema for the data to be imported. Sqoop uses MapReduce to import and export the data, which provides parallel operation as well as fault tolerance.


# Actions that take place with Sqoop command:
# =============================================
# Sqoop connects to the database to fetch the table metadata, the number of columns, names and their datatypes.
# Depending on the particular database system, other useful metadata like partitioned table etc are also retrieved.
# At this point, Sqoop is not transfering any data between database and HDFS, it is only querying catalog of tables and views.
# Based on retrieved metadata, Sqoop generates a JAVA class and compile it using the JDK and Hadoop libraries.
# Sqoop connects to the Hadoop cluster and submits a MapReduce job. Each mapper of the job transfers a slice of table's data.
# As MapReduce executes multiple mappers at same time, Sqoop will transfer data in parallel to achieve the best possible performance by utilizing the potentia of the database server.
# Each mapper transfers the table's data directly between the database and Hadoop cluster.
# It is advised not to use resource intensive functions while fetching data from the database, as it effects performance.


# GET ALL DATABASES
# Get the list all the databases available 
sqoop list-databases 
  --connect jdbc:mysql://<server_ip> 
  --username <db_user_name> 
  --password <db_user_name_password>


# GET ALL TABLES IN A DATABASE
# List all tables available in a particular database. Here database name is rajeshk
sqoop list-tables 
  --connect jdbc:mysql://<server_ip>/rajeshk 
  --driver com.mysql.jdbc.Driver 
  --username <db_user_name> 
  --password <db_user_name_password>


# FULL IMPORT OF TABLE WITH 1 MAPPER
# Import table from RDBMS to HDFS
    # -m 1 represents that there is one mapper
    # by default there are 4 mappers used when no mapper is specified
    # this command creates file in the HDFS in the home directory for the user ~/emp/part-m-00000
    # the m represents that this is a mapper job, instead of =m --num-mappers can be used as well
    # whenever a sqoop command is executed, a map reduce job kicks start and completes the activity
sqoop import 
  --connect jdbc:mysql://<server_ip>/rajeshk 
  --driver com.mysql.jdbc.Driver 
  --username <db_user_name> 
  --password <db_user_name_password>
  --table emp 
  -m 1


# FULL IMPORT OF TABLE WITH DEFAULT MAPPERS
# Import table from RDBMS to HDFS using default mappers
  # The below command has not specified the number of mappers. 
  # So by default 4 mapper jobs are run and it creates 4 files in the home directory
  # ~/emp/part-m-00000, ~/emp/part-m-00001, ~/emp/part-m-00002, ~/emp/part-m-00003
  # due to the primary key on EMPNO, the range of EMPNO is split into 4 partitions
  # the EMPNO falling into that range of EMPNOs goes into that specific partitioned file.
sqoop import 
  --connect jdbc:mysql://<server_ip>/rajeshk 
  --driver com.mysql.jdbc.Driver 
  --username <db_user_name> 
  --password <db_user_name_password>
  --table emp 


# FULL IMPORT OF TABLE WITH SPLIT BY
# Import table from RDBMS to HDFS using split-by
  # When there is no primary key on a table and only one partition is required then it's fine.
  # When there is no primary key on a table and more than one partition is required, then there should be a split-by clause, else the Sqoop job would fail.
  # The split-by clause splits the input data set into different ranges based on their values
  # Having more than one partition without primary key and without split-by leads to error
  # 'num-mappers' is same as 'm'
sqoop import 
  --connect jdbc:mysql://<server_ip>/rajeshk 
  --driver com.mysql.jdbc.Driver 
  --username <db_user_name> 
  --password <db_user_name_password>
  --table emp 
  --split-by empno 
  --num-mappers 2


# FULL IMPORT OF TABLE INTO A DIRECTORY
# Import table from RDBMS to HDFS in particular directory
  # By default the HDFS files are created in the home directory of the user
  # If the directory already exists, the job fails with the message that the folder already exists.
  # Select a destination folder name that doesn't exist
  # '--target-dir' specifies the destination directory name
  # 'num-mappers' is same as 'm'
sqoop import 
  --connect jdbc:mysql://<server_ip>/rajeshk 
  --driver com.mysql.jdbc.Driver 
  --username <db_user_name> 
  --password <db_user_name_password>
  --table emp 
  --split-by empno 
  --target-dir hdata/sqoop_demo
  --num-mappers 2


# FULL IMPORT OF TABLE INTO A WAREHOUSE DIRECTORY
# Import table from RDBMS to HDFS in particular directory
  # By default the HDFS files are created in the home directory of the user
  # If the directory already exists, the job fails with the message that the folder already exists.
  # Select a destination folder name that doesn't exist
  # '--target-dir' specifies the destination directory name
  # If you want to run multiple sqoop jobs for multiple tables, we need to change --target-dir multiple times
  # This can be avoided using --warehouse-dir. It is a directory under which a separate directory gets created with same name as
  # the table name. So, any number of tables can be accommodated.
  # 'num-mappers' is same as 'm'
  # below command creates a directory warehouse. Inside warehouse there will be another directory same as table name emp.
sqoop import 
  --connect jdbc:mysql://<server_ip>/rajeshk 
  --driver com.mysql.jdbc.Driver 
  --username <db_user_name> 
  --password <db_user_name_password>
  --table emp 
  --split-by empno 
  --warehouse-dir warehouse
  --num-mappers 2


# SELECTIVE IMPORT FROM A TABLE WITH WHERE
# Incremental Import table from RDBMS to HDFS
  # This helps in retrieving only rows newer than some previously imported set of rows
  # Two modes of incremental import - append and lastmodified
  # Use append when newer rows are continually being added with increasing rowid values
  # '--check-column' specifies the column to be checked and '--last-value' specifies the last value available in HDFS
  # lastmodified mode works if there is a timestamp that captures latest updates happened on a table
  # run the last three insert statements from the file 01_sql_statements.sql to test the incremental load
sqoop import 
  --connect jdbc:mysql://<server_ip>/rajeshk 
  --driver com.mysql.jdbc.Driver 
  --username <db_user_name> 
  --password <db_user_name_password>
  --table emp 
  --num-mappers 2
  --where "<where_condition>"


# INCREMENTAL IMPORT BASED ON A KEY AND LAST VALUE
# Incremental Import table from RDBMS to HDFS
  # This helps in retrieving only rows newer than some previously imported set of rows
  # Two modes of incremental import - append and lastmodified
  # Use append when newer rows are continually being added with increasing rowid values
  # '--check-column' specifies the column to be checked and '--last-value' specifies the last value available in HDFS
  # lastmodified mode works if there is a timestamp that captures latest updates happened on a table
  # run the last three insert statements from the file 01_sql_statements.sql to test the incremental load
sqoop import 
  --connect jdbc:mysql://<server_ip>/rajeshk 
  --driver com.mysql.jdbc.Driver 
  --username <db_user_name> 
  --password <db_user_name_password>
  --table emp 
  --split-by empno 
  --target-dir hdata/sqoop_demo
  --incremental append
  --check-column empno
  --last-value 7934
  
  
# Import table data from RDBMS to HDFS with joins - free form query imports
  # Instead of importing whole table, it is possible to define a query with selected columns as well
  # Also, we can join tables and pick up the required columns from multiple tables
  # the target-dir is mandatory while importing a free form query
  # while importing query results in parallel, then each map task will need to execute a copy of the query with results partitioned by bounding conditions in sqoop
  # Hence $CONDITIONS token need to be used which each Sqoop process will replace with a unique condition expression
  # Also a column needs to be included for --split-by
sqoop import 
  --connect jdbc:mysql://<server_ip>/rajeshk 
  --driver com.mysql.jdbc.Driver 
  --username <db_user_name> 
  --password <db_user_name_password>
  --target-dir /user/rajesh.kancharla_outlook/sqoopdemo
  --query 'select ename,job,sal,emp.deptno from emp join dept on emp.deptno = dept.deptno WHERE $CONDITIONS'
  --split-by emp.deptno
  --num-mappers 1

sqoop import 
  --connect jdbc:mysql://<server_ip>/rajeshk 
  --driver com.mysql.jdbc.Driver 
  --username <db_user_name> 
  --password <db_user_name_password>
  --target-dir /user/rajesh.kancharla_outlook/sqoopdemo
  --query "select ename,job,sal,emp.deptno from emp where emp.deptno = 30 AND \$CONDITIONS"
  --num-mappers 1


# Import table from RDBMS to Hive
  # Though Sqoop's main role is to get data from RDBMS to HDFS, it can also be used to import data into Hive
  # Sqoop generates and executes a CREATE TABLE statement to define the data's layout in Hive
  # If Hive table is already present, --hive-overwrite option indicates that table needs to be replaced
  # As part of this import, first data gets copied from RDBMS to HDFS and then data moves from HDFS to Hive Warehouse directory
  # before running the below statement, in hive a database by name hive_rajeshk has been created
sqoop import 
  --connect jdbc:mysql://<server_ip>/rajeshk 
  --driver com.mysql.jdbc.Driver 
  --username <db_user_name> 
  --password <db_user_name_password>
  --table emp 
  --num-mappers 2
  --hive-import
  --hive-table hive_rajeshk.emp_hive


# Export table from HDFS to RDBMS
  # This helps in exporting the data from HDFS to RDBMS
  # Data will be exported to a table that has no data in it
  # Data will be exported from a directory having multiple mapper files
  # Data can also be exported from a specific mapper file in a directory
  # There are many options available like insert / update. The options can be chosen according to use case
sqoop export 
  --connect jdbc:mysql://<server_ip>/rajeshk 
  --driver com.mysql.jdbc.Driver 
  --username <db_user_name> 
  --password <db_user_name_password>
  --table emp_export 
  --export-dir emp

sqoop export 
  --connect jdbc:mysql://<server_ip>/rajeshk 
  --driver com.mysql.jdbc.Driver 
  --username <db_user_name> 
  --password <db_user_name_password>
  --table emp_export 
  --export-dir /user/rajesh.kancharla_outlook/emp/part-m-00001
