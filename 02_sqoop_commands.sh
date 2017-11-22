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


# CHECK ACCESS ON TABLES
# Run a simple SQL query to check whether the table's data is accessible
# It is also possible to run stored procedures/functions using eval command
sqoop eval 
  --connect jdbc:mysql://<server_ip>/rajeshk 
  --driver com.mysql.jdbc.Driver 
  --username <db_user_name> 
  --password <db_user_name_password>
  --query "<db_query>"


# IMPORT ALL TABLES IN A DATABASE
# Import all the tables residing in a database
sqoop import-all-tables
  --connect jdbc:mysql://<server_ip>/rajeshk 
  --driver com.mysql.jdbc.Driver 
  --username <db_user_name> 
  --password <db_user_name_password>
  

# IMPORT ALL TABLES IN A DATABASE EXCEPT A FEW
# Import all the tables residing in a database except a few
# Pass the table names to be excluded to the parameter exclude-tables
# --target-dir can't be used in this case
# --warehouse-dir can be used
sqoop import-all-tables
  --connect jdbc:mysql://<server_ip>/rajeshk 
  --driver com.mysql.jdbc.Driver 
  --username <db_user_name> 
  --password <db_user_name_password>
  --exclude-tables <comma_separated_table_names>
  
  
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
# Retrieve rows matching a condition
  # This helps in retrieving only rows that match a condition in the base table.
  # Where condition can be a combination of several conditions on different columns of the table
sqoop import 
  --connect jdbc:mysql://<server_ip>/rajeshk 
  --driver com.mysql.jdbc.Driver 
  --username <db_user_name> 
  --password <db_user_name_password>
  --table emp 
  --num-mappers 2
  --where "<where_condition>"


# PASSWORD LESS SQOOP COMMAND
# If the password for the database schema is specified in the command itself, then it is easily retrievable from OS level.
# To enhance security, -P option can be used so that the password will be keyed in by user upon prompt
# One more option is to get the password from a file which has access restrictions.
sqoop import -P
  --connect jdbc:mysql://<server_ip>/rajeshk 
  --driver com.mysql.jdbc.Driver 
  --username <db_user_name> 
  --table emp 
  --num-mappers 1


# BINARY FILE FORMATS
# Sqoop supports three file formats - text/csv, Avro, Hadoop's Sequence File. Latter two are Binary
# Sequence File: It is a special Hadoop format for storing objects and implements Writable interface.
#                Customized for Map Reduce and expects each record as key-value pairs

sqoop import 
  --connect jdbc:mysql://<server_ip>/rajeshk 
  --driver com.mysql.jdbc.Driver 
  --username <db_user_name> 
  --password <db_user_name_password>
  --table emp
  --as-sequencefile

# Avro Data File: A generic system that can store any arbitrary data structure.
#                 It uses a Schema to describe what data structures are stored in the file. The schema is encoded as a JSON string 
#                 All files are created with an extension of .avro

sqoop import 
  --connect jdbc:mysql://<server_ip>/rajeshk 
  --driver com.mysql.jdbc.Driver 
  --username <db_user_name> 
  --password <db_user_name_password>
  --table emp
  --as-avrodatafile


# COMPRESS FILES
# Sqoop takes advantage of inherent parallelism of Hadoop by leveraging Hadoop's execution engine, MapReduce to perform data transfers.
# As MapReduce already has excellent support for compression, Sqoop simply reuses its powerful abilities to provide compression options.
# All files are created with .gz extension

sqoop import 
  --connect jdbc:mysql://<server_ip>/rajeshk 
  --driver com.mysql.jdbc.Driver 
  --username <db_user_name> 
  --password <db_user_name_password>
  --table emp
  --compress


# COMPRESS FILES 
# In case you don't want to use the default compression of .gz, you can specify the specific compression technique
# --compression-codec org.apache.hadoop.io.compress.SnappyCodec: This is one of the techniques
# this creates the files with extension as .snappy. The files are smaller in size compared to normal files

sqoop import 
  --connect jdbc:mysql://<server_ip>/rajeshk 
  --driver com.mysql.jdbc.Driver 
  --username <db_user_name> 
  --password <db_user_name_password>
  --table emp
  --compress
  --compression-codec org.apache.hadoop.io.compress.SnappyCodec

# SPEED UP TRANSFER
# Rather than using JDBC interface for transferring data, direct mode delegates the job of transferring data to the native utilities provided by database vendor.
# MySQL uses mysqldump and mysqlimport etc. for other database vendors
# Produces only text output. Sequence File and Avro are not supported

sqoop import 
  --connect jdbc:mysql://<server_ip>/rajeshk 
  --driver com.mysql.jdbc.Driver 
  --username <db_user_name> 
  --password <db_user_name_password>
  --table emp
  --direct


# OVERRIDE TYPE MAPPING
# The default mapping works, however in order to override the mapping, the --map-column-java parameter is used.
# This parameter takes comma separated values as key-value pairs.
sqoop import 
  --connect jdbc:mysql://<server_ip>/rajeshk 
  --driver com.mysql.jdbc.Driver 
  --username <db_user_name> 
  --password <db_user_name_password>
  --table emp
  --map-column-java empid=float


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
  --check-column <column_name>
  --last-value <column_value>


# PRESERVE LAST IMPORTED VALUE
# Using incremental import needs to refer last imported value
  # Sqoop Metastore allows us to save all parametes for later use
  # It allows to retain job definitions and to easily run them anytime
sqoop job 
  --create <job_name> 
  -- 
  import 
  --connect jdbc:mysql://<server_ip>/rajeshk 
  --driver com.mysql.jdbc.Driver 
  --username <db_user_name> 
  --password <db_user_name_password>
  --table emp 
  --incremental append 
  --check-column <column_name> 
  --last-value <column_value>  

This returns all the retained jobs
   sqoop job --list
This deletes job definitions no longer required
   sqoop job --delete <job_name>
This shows the saved job definitions
   sqoop job --show <job_name>
  

# FREE FORM QUERY IMPORT
# Import table data from RDBMS to HDFS with joins
  # Instead of importing whole table, it is possible to define a query with selected columns as well
  # Also, we can join tables and pick up the required columns from multiple tables
  # In the Free Form Query, Sqoop can't use the database catalogue to fetch the metadata
  # Table import might be faster than the free form query import
  # the query specifies the query to be used
  # the split-by is required to be specified. Generally the Primary key on the table is specified.
  # the target-dir is mandatory while importing a free form query
  # While importing query results in parallel, then each map task will need to execute a copy of the query with results partitioned by bounding conditions in sqoop
  # Hence $CONDITIONS token need to be used which each Sqoop process will replace with a unique condition expression
  # if the join query is complex, it is better to store the data into a temporary table and access it in the query
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


# CUSTOM BOUNDARIES
# Free Form Import - Specify Boundaries for Split By Column
  # boundary-query parameter specifies the min and max values to be used by the split-by parameter so that it can run multiple threads
  # the boundary query should only return 2 values which are min and max
sqoop import 
  --connect jdbc:mysql://<server_ip>/rajeshk 
  --driver com.mysql.jdbc.Driver 
  --username <db_user_name> 
  --password <db_user_name_password>
  --target-dir /user/rajesh.kancharla_outlook/sqoopdemo
  --query 'select ename,job,sal,emp.deptno from emp join dept on emp.deptno = dept.deptno WHERE $CONDITIONS'
  --split-by emp.deptno
  --boundary-query "select min(deptno), max(deptno) from emp"


# RENAME FREE FORM JOB NAMES
  # When the job is triggered, all jobs performing free form import use same name QueryResult.jar
  # In order to identify which job is running, it is possible to define a custom name for the MapReduce job
  # --mapreduce-job-name parameter is useful in this scenario
sqoop import 
  --connect jdbc:mysql://<server_ip>/rajeshk 
  --driver com.mysql.jdbc.Driver 
  --username <db_user_name> 
  --password <db_user_name_password>
  --target-dir /user/rajesh.kancharla_outlook/sqoopdemo
  --query 'select ename,job,sal,emp.deptno from emp join dept on emp.deptno = dept.deptno WHERE $CONDITIONS'
  --split-by emp.deptno
  --boundary-query "select min(deptno), max(deptno) from emp"
  --mapreduce-job-name <custom_job_name>
  
  
# IMPORT DIRECTLY TO HIVE
# Import table from RDBMS to Hive Default database
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

# Import into specific Hive database
sqoop import 
  --connect jdbc:mysql://<server_ip>/rajeshk 
  --driver com.mysql.jdbc.Driver 
  --username <db_user_name> 
  --password <db_user_name_password>
  --table emp 
  --num-mappers 2
  --hive-import
  --hive-database <database_name>

# Import into specific custom table in Hive database
sqoop import 
  --connect jdbc:mysql://<server_ip>/rajeshk 
  --driver com.mysql.jdbc.Driver 
  --username <db_user_name> 
  --password <db_user_name_password>
  --table emp 
  --num-mappers 2
  --hive-import
  --hive-database <database_name>
  --hive-table <custom_table_name>

# Change datatypes of the columns in hive table
sqoop import 
  --connect jdbc:mysql://<server_ip>/rajeshk 
  --driver com.mysql.jdbc.Driver 
  --username <db_user_name> 
  --password <db_user_name_password>
  --table emp 
  --num-mappers 2
  --hive-import
  --hive-database <database_name>
  --hive-table <custom_table_name>
  --map-column-hive <col_name>=<type_name>,..

# Import into Hive Partitioned table.
  # Partition column should be of type String
  # Supports only one level of partitioning not multi-level
sqoop import 
  --connect jdbc:mysql://<server_ip>/rajeshk 
  --driver com.mysql.jdbc.Driver 
  --username <db_user_name> 
  --password <db_user_name_password>
  --table emp 
  --num-mappers 2
  --hive-import
  --hive-database <database_name>
  --hive-table <custom_table_name>
  --hive-partition-key <key_column_name>
  --hive-partition-value <key_column_value>

# Handling de-limiters
  # When the data itself contains characters that are used as Hive's delimiters, it often results in data mismatches
  # Sqoop can automatically clean data using --hive-drop-import-delims parameter
  # It removes all \n, \t, \01 characters from string based columns
sqoop import 
  --connect jdbc:mysql://<server_ip>/rajeshk 
  --driver com.mysql.jdbc.Driver 
  --username <db_user_name> 
  --password <db_user_name_password>
  --table emp 
  --num-mappers 2
  --hive-import
  --hive-database <database_name>
  --hive-table <custom_table_name>
  --hive-drop-import-delims

  # If delimiters are not to be dropped, instead need to be replaced with a string, use --hive-delims-replacement
sqoop import 
  --connect jdbc:mysql://<server_ip>/rajeshk 
  --driver com.mysql.jdbc.Driver 
  --username <db_user_name> 
  --password <db_user_name_password>
  --table emp 
  --num-mappers 2
  --hive-import
  --hive-database <database_name>
  --hive-table <custom_table_name>
  --hive-delims-replacement



# EXPORT DATA FROM HDFS TO DATABASE
# Export table from HDFS to RDBMS
  # This helps in exporting the data from HDFS to RDBMS
  # Data will be exported to a table that has no data in it (or) already having data in it
  # Data will be exported from a directory having multiple mapper files
  # Insert statements are created internally by the MapReduce program and lands data in the database
sqoop export 
  --connect jdbc:mysql://<server_ip>/rajeshk 
  --driver com.mysql.jdbc.Driver 
  --username <db_user_name> 
  --password <db_user_name_password>
  --table emp_export 
  --export-dir emp

# Export specific file's contents only instead of full directory
sqoop export 
  --connect jdbc:mysql://<server_ip>/rajeshk 
  --driver com.mysql.jdbc.Driver 
  --username <db_user_name> 
  --password <db_user_name_password>
  --table emp_export 
  --export-dir /user/rajesh.kancharla_outlook/emp/part-m-00001

# If inserts take long, the data can be inserted using BATCH
sqoop export 
  --connect jdbc:mysql://<server_ip>/rajeshk 
  --driver com.mysql.jdbc.Driver 
  --username <db_user_name> 
  --password <db_user_name_password>
  --table emp_export 
  --export-dir emp
  --batch

# We can also specify the number of records that will be used in each insert statement. Default is 100
sqoop export 
  -Dsqoop.export.records.per.statement=10
  --connect jdbc:mysql://<server_ip>/rajeshk 
  --driver com.mysql.jdbc.Driver 
  --username <db_user_name> 
  --password <db_user_name_password>
  --table emp_export 
  --export-dir emp

# We can also specify the number of records that will be used in each transaction. Default is 100
sqoop export 
  -Dsqoop.export.statements.per.transaction=10
  --connect jdbc:mysql://<server_ip>/rajeshk 
  --driver com.mysql.jdbc.Driver 
  --username <db_user_name> 
  --password <db_user_name_password>
  --table emp_export 
  --export-dir emp

# Inserting into a Staging Table and then to Main table
# Staging Table and Main table should have the same structure
# Data inserts into Staging first. Only after all data is loaded into Staging successfully, it lands into Main Table.
# There should be enough space to store data duplicates in both staging and main tables
sqoop export 
  --connect jdbc:mysql://<server_ip>/rajeshk 
  --driver com.mysql.jdbc.Driver 
  --username <db_user_name> 
  --password <db_user_name_password>
  --export-dir emp
  --table emp_export 
  --staging_table stg_emp_export

# Updates of data rather than Inserts.
# If there are any incremental changes done after previous inserts, they can be updated.
sqoop export 
  --connect jdbc:mysql://<server_ip>/rajeshk 
  --driver com.mysql.jdbc.Driver 
  --username <db_user_name> 
  --password <db_user_name_password>
  --table emp_export 
  --export-dir emp
  --update-key empno
  
  # Perform both Updates and Inserts
  sqoop export 
  --connect jdbc:mysql://<server_ip>/rajeshk 
  --driver com.mysql.jdbc.Driver 
  --username <db_user_name> 
  --password <db_user_name_password>
  --table emp_export 
  --export-dir emp
  --update-key empno
  --update-mode allowinsert
 
 
   # Consider only selected columns
   # By default Sqoop expects same number of columns in database table and HDFS mapper file
   # In case the HDFS has lesser number of columns than the database table, use columns parameter to specify required columns in database table that are in HDFS
  sqoop export 
  --connect jdbc:mysql://<server_ip>/rajeshk 
  --driver com.mysql.jdbc.Driver 
  --username <db_user_name> 
  --password <db_user_name_password>
  --table emp_export 
  --export-dir emp
  --columns <col1>, <col2>

  # Invoking a Stored Procedure instead of Updates/Inserts
  # Sqoop makes multiple mapper calls to Database Stored Procedure and all run in parallel
  # Good to keep login in SP as minimal as possible to maximise the performance
  sqoop export 
  --connect jdbc:mysql://<server_ip>/rajeshk 
  --driver com.mysql.jdbc.Driver 
  --username <db_user_name> 
  --password <db_user_name_password>
  --table emp_export 
  --export-dir emp
  --call <db_procedure_name>
  
  
 
