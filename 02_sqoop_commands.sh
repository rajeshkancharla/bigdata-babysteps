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
    # the m represents that this is a mapper job
sqoop import 
  --connect jdbc:mysql://<server_ip>/rajeshk 
  --driver com.mysql.jdbc.Driver 
  --username <db_user_name> 
  --password <db_user_name_password>
  --table emp 
  -m 1


