# Use pyspark to launch spark in the python shell
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

# ==============================================================================================================================================
# mini project for getting orders and order items details
# get the number of orders and amount for each dat

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
