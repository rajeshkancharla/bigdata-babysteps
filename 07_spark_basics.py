# Use pyspark to launch spark in the python shell
pyspark --master yarn --conf spark.ui.port=12888
# by default a spark context object is provided, generally as sc.
# in order to get a SQL Context use
from pyspark.sql import SQLContext
# in order to get a Hive Context use
from pyspark.sql import HiveContext
# create the sqlContext variable using
sqlContext = HiveContext(sc)


# This file contain basic Python commands using pyspark

>>> sampleList = [1,2,3,4,5]

>>> sampleList
[1, 2, 3, 4, 5]

>>> sampleRDD = sc.parallelize(sampleList)

>>> sampleRDD
ParallelCollectionRDD[6] at parallelize at PythonRDD.scala:423

>>> type(sampleRDD)
<class 'pyspark.rdd.RDD'>

>>> sampleRDD.collect()
[1, 2, 3, 4, 5]

>>> sampleRDD.map(lambda x: x*x).collect()
[1, 4, 9, 16, 25]

>>> sampleRDD.map(lambda x: x * x).filter(lambda x: x%2 == 0).collect()
[4, 16]


# the below file is stored in the HDFS
#[rajeshkancharla@gw01 ~]$ hdfs dfs -cat fruits.txt
#orange apple mango
#apple mango orange
#mango orange apple
#banana

>>> textRDD = sc.textFile("fruits.txt")

>>> textRDD
fruits.txt MapPartitionsRDD[11] at textFile at NativeMethodAccessorImpl.java:-2

>>> type(textRDD)
<class 'pyspark.rdd.RDD'>

>>> textRDD.flatMap(lambda line: line.split(" ")).collect()
[u'orange', u'apple', u'mango', u'apple', u'mango', u'orange', u'mango', u'orange', u'apple', u'banana']

>>> textRDD.flatMap(lambda line: line.split(" ")).map(lambda word: (word,1)).collect()
[(u'orange', 1), (u'apple', 1), (u'mango', 1), (u'apple', 1), (u'mango', 1), (u'orange', 1), (u'mango', 1), (u'orange', 1), (u'apple', 1), (u'banana', 1)]

>>> textRDD.flatMap(lambda line: line.split(" ")).map(lambda word: (word,1)).reduceByKey(lambda x, y: x + y).collect()
[(u'orange', 3), (u'mango', 3), (u'apple', 3), (u'banana', 1)]

# read the text file from HDFS
# by default it reads the files from HDFS
>>> fruits=sc.textFile("fruits.txt")

>>> type(fruits)
<class 'pyspark.rdd.RDD'>

>>> for fruit in fruits.collect():
...    print(fruit)

# read a file from local unix file system
>>> fruits_local=sc.textFile("file:///home/rajeshkancharla/fruits.txt")

>>> type(fruits_local)
<class 'pyspark.rdd.RDD'>

>>> for fruit in fruits_local.collect():
...    print(fruit)

# accessing using the fully qualified path name
[rajeshkancharla@gw01 ~]$ hadoop fs -ls hdfs://nn01.itversity.com:8020/user/rajeshkancharla/fruits.txt
-rw-r--r--   3 rajeshkancharla hdfs         64 2017-12-11 08:16 hdfs://nn01.itversity.com:8020/user/rajeshkancharla/fruits.txt

# load text file using fully qualified path name in HDFS
>>> fruits_local_full=sc.textFile("hdfs://nn01.itversity.com:8020/user/rajeshkancharla/fruits.txt")

>>> type(fruits_local_full)
<class 'pyspark.rdd.RDD'>

>>> for fruit in fruits_local_full.collect():
...    print(fruit)

# Save the file as a sequence file
>>> deptRDD = sc.textFile("sqoop_import/departments")
>>> deptRDD.map(lambda x: (None,x)).saveAsSequenceFile("/user/rajeshkancharla/pyspark/departmentSeq")
>>> deptRDD.map(lambda x: tuple(x.split(",",1))).saveAsSequenceFile("/user/rajeshkancharla/pyspark/departmentTupleSeq")

# Read the data from the sequence file
>>> deptRDD = sc.sequenceFile("pyspark/departmentTupleSeq")
>>> deptRDD.collect()

# Specifying the data types
# first one is KEY's datatype and second one is VALUE's datatype
>>> deptRDD = sc.sequenceFile("pyspark/departmentTupleSeq", "org.apache.hadoop.io.IntWritable", "org.apache.hadoop.io.Text")
>>> for dept in deptRDD.collect():
...    print(dept)
... 
(u'2', u'Fitness')
(u'3', u'Footwear')
(u'4', u'Apparel')
(u'5', u'Golf')
(u'6', u'Outdoors')
(u'7', u'Fan Shop')

# Reading data from Hive
# Create the Hive Context
from pyspark.sql import HiveContext
sqlContext = HiveContext(sc)

>>> data = sqlContext.sql("select * from rajeshk.products")
>>> for d in data.collect():
...    print(d)
... 

#JSON Files:

>>> from pyspark.sql import SqlContext
>>> sqlContext = SQLContext(sc)
>>> peopleJson = sqlContext.jsonFile("pyspark/people.json")
>>> for people in peopleJson.collect():
...    print(people)
... 

# for creating a temporary table, use
>>> peopleJson.registerTempTable("pjson")
>>> for people in sqlContext.sql("Select * from pjson").collect():
...    print(people)
... 

# writing the data to JSON format
>>> peopleData = sqlContext.sql("Select * from pjson")
>>> peopleData.toJSON().saveAsTextFile("pyspark/newJson")

# =======================================================================================================================================
# mini project for getting orders and order items details
# get the number of orders and amount for each date
# here joins and reduceByKey for number of orders and total amount are used.

# 1. Create the RDD for Orders
>>> ordersRDD = sc.textFile("sqoop_import/orders")

# 2. Create the RDD for Order Items
>>> orderItemsRDD = sc.textFile("sqoop_import/order_items")

# 3. Parse the Orders RDD and create key-value pairs, the key is the order ID and the value is full string from Orders
>>> ordersParsedRDD = ordersRDD.map(lambda x: (int(x.split(",")[0]), x))

# 4. Parse the Order Items RDD and create key-value pairs, the key is the Order Item Order ID and the value is full string from Order Items
>>> orderItemsParsedRDD = orderItemsRDD.map(lambda x: (int(x.split(",")[1]), x))

# 5. Join the two datasets Orders and Order Items
>>> orderJoinOrderItemsRDD = orderItemsParsedRDD.join(ordersParsedRDD)

# 6. Create the dataset for (date, amount)
>>> revenuePerOrderPerDay = orderJoinOrderItemsRDD.map(lambda x: (x[1][1].split(",")[1], float(x[1][0].split(",")[4])))

# 7. Include only the orders from Order Items table as Order may also have cancelled type of status
>>> ordersPerDay = orderJoinOrderItemsRDD.map(lambda x: x[1][1].split(",")[1] + "," + str(x[0])).distinct()

# 8. Make Key Value pairs for dates
>>> ordersPerDayParsedRDD = ordersPerDay.map(lambda x: (x.split(",")[0], 1))

# 9. Add all the values so that for each date, we can get number of orders
>>> totalOrdersPerDay = ordersPerDayParsedRDD.reduceByKey(lambda x,y: x+y)

#10.Get Total Revenue per Day 
>>> totalRevenuePerDay = revenuePerOrderPerDay.reduceByKey(lambda x,y: x+y)

#11.Get total orders and total revenue per day
>>> finalJoinRDD = totalOrdersPerDay.join(totalRevenuePerDay)

# this command is to check the Direct Acyclic Graph (DAG) of all the list of transformations in sequence
revenuePerOrder.toDebugString()
# ===================================================================================================================================
# Using Hive
from pyspark.sql import HiveContext
sqlContext = HiveContext(sc)
sqlContext.sql("set spark.sql.shuffle.partitions=10");

joinAggData = sqlContext.sql("select o.order_date, round(sum(oi.order_item_subtotal), 2), \
count(distinct o.order_id) from orders o join order_items oi \
on o.order_id = oi.order_item_order_id \
group by o.order_date order by o.order_date")

for data in joinAggData.collect():
  print(data)

# ===================================================================================================================================
# Using spark native sql
from pyspark.sql import SQLContext, Row
sqlContext = SQLContext(sc)
sqlContext.sql("set spark.sql.shuffle.partitions=10");

ordersRDD = sc.textFile("sqoop_import/orders")
ordersMap = ordersRDD.map(lambda o: o.split(","))
orders = ordersMap.map(lambda o: Row(order_id=int(o[0]), order_date=o[1], \
order_customer_id=int(o[2]), order_status=o[3]))
ordersSchema = sqlContext.inferSchema(orders)
ordersSchema.registerTempTable("orders")

orderItemsRDD = sc.textFile("sqoop_import/order_items")
orderItemsMap = orderItemsRDD.map(lambda oi: oi.split(","))
orderItems = orderItemsMap.map(lambda oi: Row(order_item_id=int(oi[0]), order_item_order_id=int(oi[1]), \
order_item_product_id=int(oi[2]), order_item_quantity=int(oi[3]), order_item_subtotal=float(oi[4]), \
order_item_product_price=float(oi[5])))
orderItemsSchema = sqlContext.inferSchema(orderItems)
orderItemsSchema.registerTempTable("order_items")

joinAggData = sqlContext.sql("select o.order_date, sum(oi.order_item_subtotal), \
count(distinct o.order_id) from orders o join order_items oi \
on o.order_id = oi.order_item_order_id \
group by o.order_date order by o.order_date")

for data in joinAggData.collect():
  print(data)

# ===================================================================================================================================
# Get the data of maximum valued product
# reduce is an action

productsRDD = sc.textFile("sqoop_import/products")
productsMap = productsRDD.map(lambda rec: rec) 
productsMap.reduce(lambda rec1, rec2: (rec1 if(float(rec1.split(",")[4]) > float(rec2.split(",")[4])) else rec2))


# Get the orders count based on the status
ordersRDD = sc.textFile("sqoop_import/orders")
ordersMap = ordersRDD.map(lambda rec: (rec.split(",")[3], 1))
ordersMap.countByKey()

# Using ReduceByKey
ordersReduceBy = ordersMap.reduceByKey(lambda x,y: x+y)

# Using AggregateByKey
ordersAggregateBy = ordersMap.aggregateByKey(0, lambda acc, val: acc+1, lambda acc, val: acc + val)

# Using CombineByKey
ordersCombineBy = ordersMap.combineByKey(lambda val: 1, lambda acc, val: acc+1, lambda acc, val: acc + val)

# ===================================================================================================================================
# mini project for getting orders and order items details
# get the number of orders and amount for each date
# here aggregateByKey and combineByKey are used


ordersRDD = sc.textFile("sqoop_import/orders")
orderItemsRDD = sc.textFile("sqoop_import/order_items")
ordersMapRDD = ordersRDD.map(lambda rec: (int(rec.split(",")[0]), rec))
orderItemsMapRDD = orderItemsRDD.map(lambda rec: (int(rec.split(",")[1]), rec))
ordersJoinOrderItems = ordersMapRDD.join(orderItemsMapRDD)
ordersJoinOrderItemsMap = ordersJoinOrderItems.map(lambda rec: ((rec[1][0].split(",")[1], int(rec[0])), float(rec[1][1].split(",")[4])))
revenuePerDayPerOrder = ordersJoinOrderItemsMap.reduceByKey(lambda x, y: x+y)
revenuePerDayPerOrderMap = revenuePerDayPerOrder.map(lambda rec: (rec[0][0], rec[1]))

# revenuePerDay = revenuePerDayPerOrderMap.reduceByKey(lambda x,y: x+y)
# ordersPerDay = revenuePerDayPerOrderMap.countByKey()

# using combineByKey
revenuePerDay = revenuePerDayPerOrderMap.combineByKey( \
lambda x: (x, 1), \
lambda acc, revenue: (acc[0] + revenue, acc[1] + 1), \
lambda total1, total2: (round(total1[0] + total2[0], 2), total1[1] + total2[1]) \
)

# using aggregateByKey
revenuePerDay = revenuePerDayPerOrderMap.aggregateByKey( \
(0, 0), \
lambda acc, revenue: (acc[0] + revenue, acc[1] + 1), \
lambda total1, total2: (round(total1[0] + total2[0], 2), total1[1] + total2[1]) \
)

avgRevenuePerDay = revenuePerDay.map(lambda x: (x[0], x[1][0]/x[1][1]))

# ===================================================================================================================================
# Data Frames

# TEXT
sqlContext.load("/public/retail_db/order_items", "text").show()

#JSON
sqlContext.load("/public/retail_db_json/order_items", "json").show()
sqlContext.read.json("/public/retail_db_json/order_items").show()

#AVRO
ordersDF = sqlContext.read.format("com.databricks.spark.avro").load("problem1/orders")
orderItemsDF = sqlContext.read.format("com.databricks.spark.avro").load("problem1/order_items")

#Join Data Frames
orderJoin = ordersDF.join(orderItemsDF, ordersDF.order_id == orderItemsDF.order_item_order_id)


# ====================================================================================================================================
# As a Spark Program

from pyspark import SparkContext, SQLContext
conf = SparkConf().setAppName("App Name")
sc = sparkContext(conf = conf)



# ====================================================================================================================================
# Typical Life Cycle

#OPTION 1 : HIVE Table
# When there is Hive table, a data frame can directly be created using sqlContext.sql("select * from table")
>>> ordersDF = sqlContext.sql("select * from rajeshk.orders")

#OPTION 2 : MYSQL Table
# When the table is in mysql, import into HDFS using Sqoop and perform hive import
sqoop import \
  --connect jdbc:mysql://ms.itversity.com/hr_db \
  --username hr_user \
  --password itversity \
  --table orders \
  --table emp \
  --num-mappers 1 \
  --hive-import \
  --hive-database rajeshk \
  --hive-table orders
# When the Hive table is created, a data frame can directly be created using sqlContext.sql("select * from table")
>>> ordersDF = sqlContext.sql("select * from rajeshk.orders")

#OPTION 3: Local File
>>> from pyspark.sql import Row
>>> ordersRaw = open("/path/to/local/orders/file").read().splitlines()
>>> ordersRDD = sc.parallelize(productsRaw)
>>> ordersDF = ordersRDD.map(lambda rec: Row(order_id=int(rec.split(",")[0]), order_date=rec.split(",")[1], order_customer=int(rec.split(",")[2]), order_status=rec.split(",")[3])).toDF()

#OPTION 4: HDFS File
>>> from pyspark.sql import Row
>>> ordersRDD = sc.textFile("/path/to/hdfs/orders/folder")
>>> ordersDF = ordersRDD.map(lambda rec: Row(order_id=int(rec.split(",")[0]), order_date=rec.split(",")[1], order_customer=int(rec.split(",")[2]), order_status=rec.split(",")[3])).toDF()


# Data Frame Operations:
# After ordersDF is created, a temporary dataframe table can be created using ordersDF.registerTempTable("ordersDF_table")
# Data frame table data can be accessed using
>>> sqlContext.sql("select * from ordersDF_table")
# Perform the required operations on this dataframe table and store the result back into another dataframe
>>> postOrdersDF = sqlContext.sql("select count(*), order_state from ordersDF_table group by order_state")

#Write to table / hdfs
# use the DF to save to another Hive table / HDFS  
# Write dataFrame output to an existing hive table
>>> ordersDF.insertInto("rajeshk.orders_new")
# Write dataFrame output to a new table in Hive
>>>		ordersDF.saveAsTable("rajeshk.orders_new1")

# Write dataFrame output to a folder in HDFS 
# JSON
>>>	ordersDF.save("ordersJson", "json")
>>>	ordersDF.write.json("ordersJsonDirect")
dataFrame.toJSON().saveAsTextFile(<path to location>,classOf[Compression Codec])

# ORC
>>> ordersDF.save("ordersORC", "orc")
>>> ordersDF.write.orc("ordersORCDirect")		
df.write.mode(SaveMode.Overwrite).format("orc") .save(<path to location>)

# PARQUET
>>> ordersDF.save("ordersParquet", "parquet")
>>>ordersDF.write.parquet("ordersParquetDirect")

#============================================================================
# didn't work
# CSV
ordersDF.write.csv("ordersCSV", header="true", mode="overwrite")
ordersDF.write.format('com.databricks.spark.csv').mode('overwrite').option("header", "true").save("ordersCSV")

# AVRO
ordersDF.save("ordersAvro", "avro")
ordersDF.write.avro("ordersAvroDirect")
sqlContext.setConf("spark.sql.avro.compression.codec","snappy") //use snappy, deflate, uncompressed;
dataFrame.write.avro(<path to location>);


avro file
pyspark --packages com.databricks:spark-avro_2.10:2.0.1
import com.databricks.spark.avro._
import sqlContext.implicits._

df = sqlContext.read.format("com.databricks.spark.avro").load("kv.avro")


#============================================================================
# Create dataframes 

# the below command created a dataframe from a text file
orders = spark.read.csv.('filePath',sep=',').toDF('col1','col2','col3')
# in above command, the col1, col2, col3 are all part of dataframe schema and represent dataframe columns header


# the below command creates a dataframe from the underlying hive table
orders = spark.read.table('HiveTableSchema.HiveTableName')

# the below command creates a dataframe from the underlying hive table
spark.sql('select * from HiveTableSchema.HiveTableName')

# once you have a dataframe a temporary table can be created
df.createeOrReplaceTempView("table1")
spark.sql('select * from table1')  #and this will again result a dataframe                          


#============================================================================
# SPARK SQL

# to list all tables
spark.sql('show tables') # this returns a dataframe
spark.sql('show tables').show() # this shows the contents of the dataframe as a tabular form

spark.sql('select * from table1').show()  # this shows the contents of the dataframe as a tabular form

#spark run time properties are saved in /etc/spark/conf folder and it contains below key files
#  spark-env.sh
#  spark-defaults.conf
#  hive-site.xml for hive properties


