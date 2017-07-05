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

sqoop import 
  --connect jdbc:mysql://172.31.54.174/rajeshk 
  --driver com.mysql.jdbc.Driver 
  --username labuser 
  --password simplilearn 
  --table emp 
  -m 1

# -m 1 represents that there is one mapper
# by default there are 4 mappers used when no mapper is specified
# this command creates file in the HDFS in the home directory for the user ~/emp/part-m-00000
# the m represents that this is a mapper job
