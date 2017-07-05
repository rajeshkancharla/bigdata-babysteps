# About Sqoop:
# Sqoop is a tool designed to transfer data between Hadoop and relational databases or mainframes. You can use Sqoop to import data from a relational database management system (RDBMS) such as MySQL or Oracle or a mainframe into the Hadoop Distributed File System (HDFS), transform the data in Hadoop MapReduce, and then export the data back into an RDBMS.
# Sqoop automates most of this process, relying on the database to describe the schema for the data to be imported. Sqoop uses MapReduce to import and export the data, which provides parallel operation as well as fault tolerance.

# List all the databases available 
sqoop list-databases 
  --connect jdbc:mysql://<server_ip> 
  --username <db_user_name> 
  --password <db_user_name_password>


# List all tables available in a database. Here database name is rajeshk
sqoop list-tables 
  --connect jdbc:mysql://<server_ip>/rajeshk 
  --driver com.mysql.jdbc.Driver 
  --username <db_user_name> 
  --password <db_user_name_password>


# Import table from RDBMS to HDFS
    # -m 1 represents that there is one mapper
    # by default there are 4 mappers used when no mapper is specified
    # this command creates file in the HDFS in the home directory for the user ~/emp/part-m-00000
    # the m represents that this is a mapper job, instead of =m --num-mappers can be used as well
sqoop import 
  --connect jdbc:mysql://<server_ip>/rajeshk 
  --driver com.mysql.jdbc.Driver 
  --username <db_user_name> 
  --password <db_user_name_password>
  --table emp 
  -m 1


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


# Import table from RDBMS to HDFS using split-by
  # When there is no primary key on a table and only one partition is required then it's fine.
  # When there is no primary key on a table and more than one partition is required, then there should be a split-by clause
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

