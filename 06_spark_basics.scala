/*
SPARK:
======
- Analytical or processing engine
- Component which only proceses your data
- Won't provide storage to it
- For storage you can use HDFS or any other file system
- Can be used outside of hadoop as a standalone system

Architecture
=============

-----------------------------------------------------------------------------
|   SPARK SQL	|    GRAPHX    |    SPARK STREAMING	|    ML Library     |
-----------------------------------------------------------------------------
|                             SPARK CORE				    |
-----------------------------------------------------------------------------
		
Features of Spark:
===================
1. General Purpose Engine
-- MapReduce also provides all the features as Spark
-- The capabilities are in different components and compatibility is challenging
-- The entire life cycle of development, maintenance, support - becomes difficult when all capabilities are in different components
-- Spark provides are all in single package - no need to worry on individual components integration - ease of maintenance


2. In-Memory computing capability
-- Spark tries to keep data in memory as much as it can. If it can't it spills data to the disk.
-- Machine Learning are iterative in nature where we do same steps again and again on same set of data.

MR Processing:
-- Map Reduce stores intermediate result onto the disk.
-- Map Reduce job again reads data from disk and finally stores result in HDFS.
-- latency high in disk - takes more time to read data from disk.

SPARK:
-- Spark Job stores intermediate results in memory
-- reads data again from memory and finally stores result in HDFS
-- Faster than MR but how much faster depends on lot of factors - memory, more data in memory - more faster, data spilled to disk - perf. decreases.
-- Def perf is better for iterative jobs.
-- development with spark is easy
-- Higher the RAM better it is - tradeoff between cost and performance. Spend more to bet 256GB RAM and get optimal performance

Java provides APIs for the below languages:
-- SCALA / PYTHON / JAVA / R
-- Spark understands only these 4 languages
-- One of these languages is required to work with Spark
-- 90% of spark code is written in Scala. However it depends on situation which one to chose from.
-- Data scientist - Python
-- Statistician - R

*/


Transformations: map
=======================

scala> val input = sc.parallelize(List(1,2,3,4,5))
input: org.apache.spark.rdd.RDD[Int] = ParallelCollectionRDD[3] at parallelize at <console>:27

-- map maps your input to some sort of output depends on the logic provided by us.

scala> val sq = input.map(x => x * x)
sq: org.apache.spark.rdd.RDD[Int] = MapPartitionsRDD[4] at map at <console>:29

-- this is an anonymous function to do the operation of squaring the number

scala> sq.collect()
res2: Array[Int] = Array(1, 4, 9, 16, 25)

scala> input.collect()
res3: Array[Int] = Array(1, 2, 3, 4, 5)


Transformations: filter
=======================

scala> val input = sc.parallelize(List(1,2,3,4,5))
input: org.apache.spark.rdd.RDD[Int] = ParallelCollectionRDD[3] at parallelize at <console>:27

scala> val even = input.filter(x => x % 2 == 0)
even: org.apache.spark.rdd.RDD[Int] = MapPartitionsRDD[6] at filter at <console>:29

scala> even.collect()
res4: Array[Int] = Array(2, 4)


Transformations: flatMap
========================
-- similar to map
-- and additionally it flattens the structure as well

scala> val input = sc.parallelize(List(1,2,3,4,5))
input: org.apache.spark.rdd.RDD[Int] = ParallelCollectionRDD[3] at parallelize at <console>:27

scala> def g(x: Int) = List(x - 1, x, x + 1)
g: (x: Int)List[Int]

scala> g(1)
res5: List[Int] = List(0, 1, 2)

scala> val rdd1 = input.map(x => g(x))
rdd1: org.apache.spark.rdd.RDD[List[Int]] = MapPartitionsRDD[7] at map at <console>:31

scala> rdd1.collect()
res6: Array[List[Int]] = Array(List(0, 1, 2), List(1, 2, 3), List(2, 3, 4), List(3, 4, 5), List(4, 5, 6))

-- it returns a collection of collections

scala> val rdd1 = input.flatMap(x => g(x))
rdd1: org.apache.spark.rdd.RDD[Int] = MapPartitionsRDD[8] at flatMap at <console>:31

scala> rdd1.collect()
res7: Array[Int] = Array(0, 1, 2, 1, 2, 3, 2, 3, 4, 3, 4, 5, 4, 5, 6)


Transformations: reduceByKey
============================
-- works on a key value pair

scala> val input = sc.parallelize(List("hello", "world", "hello", "world"))
input: org.apache.spark.rdd.RDD[Int] = ParallelCollectionRDD[3] at parallelize at <console>:27

scala> val rdd1 = input.map(x => (x, x.length))
rdd1: org.apache.spark.rdd.RDD[(String, Int)] = MapPartitionsRDD[11] at map at <console>:29

scala> rdd1.collect()
res9: Array[(String, Int)] = Array((hello,5), (world,5), (hello,5), (world,5))

reduceByKey works in two phases
1. for a particular key, it collects all the values
2. what to do with this collection is specified in the function
   reduceByKey((x,y) => x + y)

scala> rdd1.reduceByKey((x,y) => x + y).collect()
res10: Array[(String, Int)] = Array((hello,10), (world,10))


Word count program:
====================

scala> val textFile = sc.textFile("spark_files/fruits.txt")
textFile: org.apache.spark.rdd.RDD[String] = MapPartitionsRDD[16] at textFile at <console>:27

scala> val counts = textFile.flatMap(line => line.split(" ")).map(word => (word,1)).reduceByKey((x,y) => x + y)
counts: org.apache.spark.rdd.RDD[(String, Int)] = ShuffledRDD[22] at reduceByKey at <console>:29

scala> counts.collect()
res12: Array[(String, Int)] = Array((orange,1), (apple,3), (mango,3), (banana,3))

-- doing in a single line
scala> sc.textFile("spark_files/fruits.txt").flatMap(line => line.split(" ")).map(word => (word,1)).reduceByKey((x,y) => x + y).collect()

counts.saveAsTextFile("spark_files/fruits_1.txt")

val input = sc.textFile("spark_files/fruits")
input.collect().foreach(println)


Transformations: zip
======================
-- it zips two different datasets

scala> val input = sc.parallelize(List("hello", "world", "welcome", "rajesh"))

scala> val lenrdd = input.map(x => x.length)

scala> val ziprdd = input.zip(lenrdd)
ziprdd: org.apache.spark.rdd.RDD[(String, Int)] = ZippedPartitionsRDD2[56] at zip at <console>:31

scala> ziprdd.collect().foreach(println)


Transformations: distinct
=========================
-- it returns the distinct values from a rdd

scala> val input = sc.parallelize(List("hello", "world", "welcome", "rajesh", "hello", "world", "welcome"))
input: org.apache.spark.rdd.RDD[String] = ParallelCollectionRDD[57] at parallelize at <console>:27

scala> input.distinct.collect()
res30: Array[String] = Array(hello, welcome, world, rajesh)


Transformations: mapValues
==========================
-- it maps values only based on an operation

scala> val input = sc.parallelize(List("hello", "world", "welcome", "rajesh", "hello", "world", "welcome"))
input: org.apache.spark.rdd.RDD[String] = ParallelCollectionRDD[61] at parallelize at <console>:27

scala> input.collect()
res34: Array[String] = Array(hello, world, welcome, rajesh, hello, world, welcome)

scala> val rdd1 = input.map(x => (x, x.length))
rdd1: org.apache.spark.rdd.RDD[(String, Int)] = MapPartitionsRDD[62] at map at <console>:29

scala> rdd1.collect()
res35: Array[(String, Int)] = Array((hello,5), (world,5), (welcome,7), (rajesh,6), (hello,5), (world,5), (welcome,7))

scala> val result = rdd1.mapValues(x => x * 2)
result: org.apache.spark.rdd.RDD[(String, Int)] = MapPartitionsRDD[63] at mapValues at <console>:31

scala> result.collect()
res33: Array[(String, Int)] = Array((hello,10), (world,10), (welcome,14), (rajesh,12), (hello,10), (world,10), (welcome,14))


Transformations: sort
======================
-- This transformation is used for sorting the RDDs
-- The default sequence is ascending, however it can be overridden to make it descending as well

val keywords = List((1, "all"),(6, "but"),(8, "where"), (3, "here"), (5, "there"))
keywords: List[(Int, String)] = List((1,all), (6,but), (8,where), (3,here), (5,there))

val input = sc.parallelize(keywords)
input: org.apache.spark.rdd.RDD[(Int, String)] = ParallelCollectionRDD[64] at parallelize at <console>:29

scala> input.collect().foreach(println)
(1,all)
(6,but)
(8,where)
(3,here)
(5,there)

scala> val result = input.sortByKey()
result: org.apache.spark.rdd.RDD[(Int, String)] = ShuffledRDD[67] at sortByKey at <console>:31
-- default sorting is ascending

scala> result.collect().foreach(println)
(1,all)
(3,here)
(5,there)
(6,but)
(8,where)


-- descending order

scala> val result1 = input.sortByKey(ascending=false)
result1: org.apache.spark.rdd.RDD[(Int, String)] = ShuffledRDD[70] at sortByKey at <console>:31

scala> result1.collect().foreach(println)
(8,where)
(6,but)
(5,there)
(3,here)
(1,all)


-- sorting based on column's position in the datasets

scala> val result2 = input.sortBy(x => x._2)
result2: org.apache.spark.rdd.RDD[(Int, String)] = MapPartitionsRDD[75] at sortBy at <console>:31
-- in this statement, x is a tuple of 2 columns
-- so the _2 stands for the second column

scala> result2.collect().foreach(println)
(1,all)
(6,but)
(3,here)
(5,there)
(8,where)

scala> val result3 = input.sortBy(x => x._2, ascending = false)
result3: org.apache.spark.rdd.RDD[(Int, String)] = MapPartitionsRDD[80] at sortBy at <console>:31

scala> result3.collect().foreach(println)
(8,where)
(5,there)
(3,here)
(6,but)
(1,all)


scala> val num = List((1,5,4,7),(5,8,3,1),(11,88,54,12),(5,4,87,12))
num: List[(Int, Int, Int, Int)] = List((1,5,4,7), (5,8,3,1), (11,88,54,12), (5,4,87,12))

scala> val input = sc.parallelize(num)
input: org.apache.spark.rdd.RDD[(Int, Int, Int, Int)] = ParallelCollectionRDD[81] at parallelize at <console>:29

scala> input.collect.foreach(println)
(1,5,4,7)
(5,8,3,1)
(11,88,54,12)
(5,4,87,12)

scala> input.sortBy(x => x._2).collect.foreach(println)
(5,4,87,12)
(1,5,4,7)
(5,8,3,1)
(11,88,54,12)

scala> input.sortBy(x => x._3).collect.foreach(println)
(5,8,3,1)
(1,5,4,7)
(11,88,54,12)
(5,4,87,12)

scala> input.sortBy(x => x._4, ascending = false).collect.foreach(println)
(11,88,54,12)
(5,4,87,12)
(1,5,4,7)
(5,8,3,1)


Transformations: Set Operations
===============================
	UNION		: returns all rows - not just the distincy ones. Something like union all
	INTERSECTION: returns common in both
	MINUS		: returns values present in former but not in latter

scala> val rdd1 = sc.parallelize(List("lion", "tiger", "dog", "cat"))
rdd1: org.apache.spark.rdd.RDD[String] = ParallelCollectionRDD[97] at parallelize at <console>:27

scala> val rdd2 = sc.parallelize(List("lion", "tiger", "horse", "monkey"))
rdd2: org.apache.spark.rdd.RDD[String] = ParallelCollectionRDD[98] at parallelize at <console>:27

scala> rdd1.union(rdd2).collect()
res46: Array[String] = Array(lion, tiger, dog, cat, lion, tiger, horse, monkey)

scala> rdd1.union(rdd2).distinct.collect()
res47: Array[String] = Array(monkey, cat, dog, lion, horse, tiger)

scala> rdd1.intersection(rdd2).collect()
res49: Array[String] = Array(lion, tiger)

scala> rdd1.subtract(rdd2).collect()
res53: Array[String] = Array(dog, cat)

scala> rdd2.subtract(rdd1).collect()
res54: Array[String] = Array(monkey, horse)


Transformations: Joins
======================

scala> val rdd1 = sc.parallelize(List(("hello", 1),("world", 4), ("scala", 3)))
rdd1: org.apache.spark.rdd.RDD[(String, Int)] = ParallelCollectionRDD[118] at parallelize at <console>:27

scala> val rdd2 = sc.parallelize(List(("hello", 2),("hello", 3), ("python", 4)))
rdd2: org.apache.spark.rdd.RDD[(String, Int)] = ParallelCollectionRDD[119] at parallelize at <console>:27

scala> rdd1.join(rdd2).collect().foreach(println)
(hello,(1,2))
(hello,(1,3))

scala> rdd1.leftOuterJoin(rdd2).collect().foreach(println)
(hello,(1,Some(2)))
(hello,(1,Some(3)))
(world,(4,None))
(scala,(3,None))

 scala> rdd1.rightOuterJoin(rdd2).collect().foreach(println)
(hello,(Some(1),2))
(hello,(Some(1),3))
(python,(None,4))

scala> rdd1.fullOuterJoin(rdd2).collect().foreach(println)
(hello,(Some(1),Some(2)))
(hello,(Some(1),Some(3)))
(world,(Some(4),None))
(python,(None,Some(4)))
(scala,(Some(3),None))

-- in order to change the positions, just use a map

scala> val swap = rdd1.map(x => (x._2, x._1))
swap: org.apache.spark.rdd.RDD[(Int, String)] = MapPartitionsRDD[132] at map at <console>:29

scala> swap.collect()
res59: Array[(Int, String)] = Array((1,hello), (4,world), (3,scala))


@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@
Actions:

scala> val input = sc.parallelize(List(1,2,3,4,5))
input: org.apache.spark.rdd.RDD[Int] = ParallelCollectionRDD[133] at parallelize at <console>:27

scala> input.collect()
res60: Array[Int] = Array(1, 2, 3, 4, 5)

scala> input.count()
res61: Long = 5

scala> input.take(3)
res62: Array[Int] = Array(1, 2, 3)


scala> val input = sc.parallelize(List("here", "all", "where", "but"))
input: org.apache.spark.rdd.RDD[String] = ParallelCollectionRDD[134] at parallelize at <console>:27

scala> input.take(2)
res63: Array[String] = Array(here, all)

scala> input.takeOrdered(2)
res64: Array[String] = Array(all, but)


scala> val input = sc.parallelize(List(1,2,3,2,1))
input: org.apache.spark.rdd.RDD[Int] = ParallelCollectionRDD[136] at parallelize at <console>:27

scala> val rdd1 = input.zip(input)
rdd1: org.apache.spark.rdd.RDD[(Int, Int)] = ZippedPartitionsRDD2[137] at zip at <console>:29

scala> rdd1.collect
res65: Array[(Int, Int)] = Array((1,1), (2,2), (3,3), (2,2), (1,1))

scala> rdd1.collectAsMap
res66: scala.collection.Map[Int,Int] = Map(2 -> 2, 1 -> 1, 3 -> 3)


scala> val pairs = List((3, "all"), (3, "but"), (5, "here"), (5, "there"))
pairs: List[(Int, String)] = List((3,all), (3,but), (5,here), (5,there))

scala> val input = sc.parallelize(pairs)
input: org.apache.spark.rdd.RDD[(Int, String)] = ParallelCollectionRDD[138] at parallelize at <console>:29

scala> input.collect
res67: Array[(Int, String)] = Array((3,all), (3,but), (5,here), (5,there))

scala> input.countByKey
res68: scala.collection.Map[Int,Long] = Map(3 -> 2, 5 -> 2)



SPARK SQL: Data Frames
-- an API where we can write SQL like queries to do the analysis
-- immutable objects similar to RDD

STEP-1: Load Data

scala> val input = sc.textFile("spark_files/emp.txt")
input: org.apache.spark.rdd.RDD[String] = MapPartitionsRDD[3] at textFile at <console>:27

scala> input.collect().foreach(println)
1,Mark,1000,HR
2,Peter,1200,SALES
3,Henry,1500,HR
4,Adam,2000,IT
5,Steve,2500,IT
6,Brian,2700,IT
7,Michael,3000,HR
8,Steve,10000,SALES
9,Peter,7000,HR
10,Dan,6000,BS

-- Data frame is similar to relational table
-- data split as data and schema (metadata)
-- create the schema using case class. It's a class in scala 


STEP-2: create the schema for the data to be used.

scala> case class employee(id: Int, name: String, salary: Int, dept: String)
defined class employee

-- the above statement has created the schema with predefined types.


STEP-3: Split comma separated data

-- split the data based on comma in the below statement

scala> val input_split = input.map(line => line.split(","))
input_split: org.apache.spark.rdd.RDD[Array[String]] = MapPartitionsRDD[4] at map at <console>:29

scala> input_split.collect()
res2: Array[Array[String]] = Array(Array(1, Mark, 1000, HR), Array(2, Peter, 1200, SALES), Array(3, Henry, 1500, HR), Array(4, Adam, 2000, IT), Array(5, Steve, 250
0, IT), Array(6, Brian, 2700, IT), Array(7, Michael, 3000, HR), Array(8, Steve, 10000, SALES), Array(9, Peter, 7000, HR), Array(10, Dan, 6000, BS))

-- convert into RDD of employee objects
-- in below statement, x is an Array of String. So x(0)..x(3) are all strings
-- as some of the attributes in the class are integers, so convert the strings to int using toInt. 


STEP-4: Prepare the RDD of employee objects

scala> val emprdd = input_split.map(x => employee(x(0).toInt, x(1), x(2).toInt, x(3)))
emprdd: org.apache.spark.rdd.RDD[employee] = MapPartitionsRDD[5] at map at <console>:33

-- each member of RDD is an employee object now
-- now create the data frame using the toDF


STEP-5: Convert the RDD into the dataframe

scala> val empDF = emprdd.toDF()
empDF: org.apache.spark.sql.DataFrame = [id: int, name: string, salary: int, dept: string]

-- [id: int, name: string, salary: int, dept: string] is the schema of the dataframe
-- empDF is the dataframe

scala> empDF.show()
+---+-------+------+-----+
| id|   name|salary| dept|
+---+-------+------+-----+
|  1|   Mark|  1000|   HR|
|  2|  Peter|  1200|SALES|
|  3|  Henry|  1500|   HR|
|  4|   Adam|  2000|   IT|
|  5|  Steve|  2500|   IT|
|  6|  Brian|  2700|   IT|
|  7|Michael|  3000|   HR|
|  8|  Steve| 10000|SALES|
|  9|  Peter|  7000|   HR|
| 10|    Dan|  6000|   BS|
+---+-------+------+-----+


STEP-6: Processing the DataFrame

scala> empDF.columns
res5: Array[String] = Array(id, name, salary, dept)

scala> empDF.dtypes
res6: Array[(String, String)] = Array((id,IntegerType), (name,StringType), (salary,IntegerType), (dept,StringType))

scala> empDF.printSchema()
root
 |-- id: integer (nullable = false)
 |-- name: string (nullable = true)
 |-- salary: integer (nullable = false)
 |-- dept: string (nullable = true)

-- by default the nullable is false for int and true for string

scala> empDF.schema
res8: org.apache.spark.sql.types.StructType = StructType(StructField(id,IntegerType,false), StructField(name,StringType,true), StructField(salary,IntegerType,false
), StructField(dept,StringType,true))

-- Schema is a collection of different fields - StructType
-- Each of the items in the collection are of type - StructField
-- the nullable properties can be overridden as needed.

scala> empDF.count
res20: Long = 10


STEP-7: Changes to the metadata

-- change all the column names

scala> empDF.toDF("A", "B", "C", "D").show()
+---+-------+-----+-----+
|  A|      B|    C|    D|
+---+-------+-----+-----+
|  1|   Mark| 1000|   HR|
|  2|  Peter| 1200|SALES|
|  3|  Henry| 1500|   HR|
|  4|   Adam| 2000|   IT|
|  5|  Steve| 2500|   IT|
|  6|  Brian| 2700|   IT|
|  7|Michael| 3000|   HR|
|  8|  Steve|10000|SALES|
|  9|  Peter| 7000|   HR|
| 10|    Dan| 6000|   BS|
+---+-------+-----+-----+


-- change particular column only but not all columns

scala> empDF.withColumnRenamed("ID", "A").show()
+---+-------+------+-----+
|  A|   name|salary| dept|
+---+-------+------+-----+
|  1|   Mark|  1000|   HR|
|  2|  Peter|  1200|SALES|
|  3|  Henry|  1500|   HR|
|  4|   Adam|  2000|   IT|
|  5|  Steve|  2500|   IT|
|  6|  Brian|  2700|   IT|
|  7|Michael|  3000|   HR|
|  8|  Steve| 10000|SALES|
|  9|  Peter|  7000|   HR|
| 10|    Dan|  6000|   BS|
+---+-------+------+-----+

-- to drop a column

scala> empDF.drop("ID").show()
+-------+------+-----+
|   name|salary| dept|
+-------+------+-----+
|   Mark|  1000|   HR|
|  Peter|  1200|SALES|
|  Henry|  1500|   HR|
|   Adam|  2000|   IT|
|  Steve|  2500|   IT|
|  Brian|  2700|   IT|
|Michael|  3000|   HR|
|  Steve| 10000|SALES|
|  Peter|  7000|   HR|
|    Dan|  6000|   BS|
+-------+------+-----+

-- select a specific column(s)

scala> empDF.select(empDF("ID"),empDF("Name")).show()
+---+-------+
| ID|   Name|
+---+-------+
|  1|   Mark|
|  2|  Peter|
|  3|  Henry|
|  4|   Adam|
|  5|  Steve|
|  6|  Brian|
|  7|Michael|
|  8|  Steve|
|  9|  Peter|
| 10|    Dan|
+---+-------+


-- get distinct values from a column

scala> empDF.select(empDF("Dept")).distinct.show()
+-----+
| Dept|
+-----+
|SALES|
|   HR|
|   BS|
|   IT|
+-----+


-- to filter the dataframe 

scala> empDF.filter(empDF("Dept") === "HR").show()
+---+-------+------+----+
| id|   name|salary|dept|
+---+-------+------+----+
|  1|   Mark|  1000|  HR|
|  3|  Henry|  1500|  HR|
|  7|Michael|  3000|  HR|
|  9|  Peter|  7000|  HR|
+---+-------+------+----+

-- one more way to filter the values

scala> empDF.filter($"Dept" === "HR").show()
+---+-------+------+----+
| id|   name|salary|dept|
+---+-------+------+----+
|  1|   Mark|  1000|  HR|
|  3|  Henry|  1500|  HR|
|  7|Michael|  3000|  HR|
|  9|  Peter|  7000|  HR|
+---+-------+------+----+

-- filter the values and select only the required columns

scala> empDF.filter(empDF("Dept") === "HR").select(empDF("Id"),empDF("Name")).show()
+---+-------+
| Id|   Name|
+---+-------+
|  1|   Mark|
|  3|  Henry|
|  7|Michael|
|  9|  Peter|
+---+-------+


STEP-8: Joins of data frames

-- create a new data set

scala> val input_dept = sc.textFile("spark_files/dept.txt")
input_dept: org.apache.spark.rdd.RDD[String] = MapPartitionsRDD[11] at textFile at <console>:27

scala> input_dept.collect().foreach(println)
HR,Human Resource
SALES,Sales Department
IT,Information Technology
FC,Facilities

scala> case class dept(did: String, dname: String)
defined class dept

scala> val input_dept_split = input_dept.map(line => line.split(","))
input_dept_split: org.apache.spark.rdd.RDD[Array[String]] = MapPartitionsRDD[12] at map at <console>:29

scala> val deptrdd = input_dept_split.map(x => dept(x(0), x(1)))
deptrdd: org.apache.spark.rdd.RDD[dept] = MapPartitionsRDD[13] at map at <console>:33

scala> val deptDF = deptrdd.toDF()
deptDF: org.apache.spark.sql.DataFrame = [did: string, dname: string]

scala> deptDF.collect()
res18: Array[org.apache.spark.sql.Row] = Array([HR,Human Resource], [SALES,Sales Department], [IT,Information Technology], [FC,Facilities])

scala> deptDF.count
res19: Long = 4

scala> deptDF.show()
+-----+--------------------+
|  did|               dname|
+-----+--------------------+
|   HR|      Human Resource|
|SALES|    Sales Department|
|   IT|Information Techn...|
|   FC|          Facilities|
+-----+--------------------+


-- INNER Joins

scala> empDF.join(deptDF, empDF("dept") === deptDF("did")).show()
+---+-------+------+-----+-----+--------------------+                           
| id|   name|salary| dept|  did|               dname|
+---+-------+------+-----+-----+--------------------+
|  2|  Peter|  1200|SALES|SALES|    Sales Department|
|  8|  Steve| 10000|SALES|SALES|    Sales Department|
|  1|   Mark|  1000|   HR|   HR|      Human Resource|
|  3|  Henry|  1500|   HR|   HR|      Human Resource|
|  7|Michael|  3000|   HR|   HR|      Human Resource|
|  9|  Peter|  7000|   HR|   HR|      Human Resource|
|  4|   Adam|  2000|   IT|   IT|Information Techn...|
|  5|  Steve|  2500|   IT|   IT|Information Techn...|
|  6|  Brian|  2700|   IT|   IT|Information Techn...|
+---+-------+------+-----+-----+--------------------+

-- when the length of a string is exceeding certain length, it shows ...
-- if you want full string


scala> empDF.join(deptDF, empDF("dept") === deptDF("did")).show(truncate=false)
+---+-------+------+-----+-----+----------------------+                         
|id |name   |salary|dept |did  |dname                 |
+---+-------+------+-----+-----+----------------------+
|2  |Peter  |1200  |SALES|SALES|Sales Department      |
|8  |Steve  |10000 |SALES|SALES|Sales Department      |
|1  |Mark   |1000  |HR   |HR   |Human Resource        |
|3  |Henry  |1500  |HR   |HR   |Human Resource        |
|7  |Michael|3000  |HR   |HR   |Human Resource        |
|9  |Peter  |7000  |HR   |HR   |Human Resource        |
|4  |Adam   |2000  |IT   |IT   |Information Technology|
|5  |Steve  |2500  |IT   |IT   |Information Technology|
|6  |Brian  |2700  |IT   |IT   |Information Technology|
+---+-------+------+-----+-----+----------------------+


LEFT OUTER JOIN

-- there will be a "left_outer" in the statement

scala> empDF.join(deptDF, empDF("dept") === deptDF("did"),"left_outer").show(truncate=false)
+---+-------+------+-----+-----+----------------------+
|id |name   |salary|dept |did  |dname                 |
+---+-------+------+-----+-----+----------------------+
|2  |Peter  |1200  |SALES|SALES|Sales Department      |
|8  |Steve  |10000 |SALES|SALES|Sales Department      |
|1  |Mark   |1000  |HR   |HR   |Human Resource        |
|3  |Henry  |1500  |HR   |HR   |Human Resource        |
|7  |Michael|3000  |HR   |HR   |Human Resource        |
|9  |Peter  |7000  |HR   |HR   |Human Resource        |
|10 |Dan    |6000  |BS   |null |null                  |
|4  |Adam   |2000  |IT   |IT   |Information Technology|
|5  |Steve  |2500  |IT   |IT   |Information Technology|
|6  |Brian  |2700  |IT   |IT   |Information Technology|
+---+-------+------+-----+-----+----------------------+

RIGHT OUTER JOIN

-- there will be a "right_outer" in the statement

scala> empDF.join(deptDF, empDF("dept") === deptDF("did"),"right_outer").show(truncate=false)
+----+-------+------+-----+-----+----------------------+
|id  |name   |salary|dept |did  |dname                 |
+----+-------+------+-----+-----+----------------------+
|2   |Peter  |1200  |SALES|SALES|Sales Department      |
|8   |Steve  |10000 |SALES|SALES|Sales Department      |
|1   |Mark   |1000  |HR   |HR   |Human Resource        |
|3   |Henry  |1500  |HR   |HR   |Human Resource        |
|7   |Michael|3000  |HR   |HR   |Human Resource        |
|9   |Peter  |7000  |HR   |HR   |Human Resource        |
|4   |Adam   |2000  |IT   |IT   |Information Technology|
|5   |Steve  |2500  |IT   |IT   |Information Technology|
|6   |Brian  |2700  |IT   |IT   |Information Technology|
|null|null   |null  |null |FC   |Facilities            |
+----+-------+------+-----+-----+----------------------+

FULL OUTER JOIN

-- there will be a "full_outer" in the statement

scala> empDF.join(deptDF, empDF("dept") === deptDF("did"),"full_outer").show(truncate=false)
+----+-------+------+-----+-----+----------------------+
|id  |name   |salary|dept |did  |dname                 |
+----+-------+------+-----+-----+----------------------+
|2   |Peter  |1200  |SALES|SALES|Sales Department      |
|8   |Steve  |10000 |SALES|SALES|Sales Department      |
|1   |Mark   |1000  |HR   |HR   |Human Resource        |
|3   |Henry  |1500  |HR   |HR   |Human Resource        |
|7   |Michael|3000  |HR   |HR   |Human Resource        |
|9   |Peter  |7000  |HR   |HR   |Human Resource        |
|10  |Dan    |6000  |BS   |null |null                  |
|4   |Adam   |2000  |IT   |IT   |Information Technology|
|5   |Steve  |2500  |IT   |IT   |Information Technology|
|6   |Brian  |2700  |IT   |IT   |Information Technology|
|null|null   |null  |null |FC   |Facilities            |
+----+-------+------+-----+-----+----------------------+


SORT

-- default is ascending order

scala> empDF.orderBy(empDF("Id")).show()
+---+-------+------+-----+
| id|   name|salary| dept|
+---+-------+------+-----+
|  1|   Mark|  1000|   HR|
|  2|  Peter|  1200|SALES|
|  3|  Henry|  1500|   HR|
|  4|   Adam|  2000|   IT|
|  5|  Steve|  2500|   IT|
|  6|  Brian|  2700|   IT|
|  7|Michael|  3000|   HR|
|  8|  Steve| 10000|SALES|
|  9|  Peter|  7000|   HR|
| 10|    Dan|  6000|   BS|
+---+-------+------+-----+

-- descending order takes the key word desc next to the column on which sorting is required

scala> empDF.orderBy(empDF("salary").desc).show()
+---+-------+------+-----+
| id|   name|salary| dept|
+---+-------+------+-----+
|  8|  Steve| 10000|SALES|
|  9|  Peter|  7000|   HR|
| 10|    Dan|  6000|   BS|
|  7|Michael|  3000|   HR|
|  6|  Brian|  2700|   IT|
|  5|  Steve|  2500|   IT|
|  4|   Adam|  2000|   IT|
|  3|  Henry|  1500|   HR|
|  2|  Peter|  1200|SALES|
|  1|   Mark|  1000|   HR|
+---+-------+------+-----+

-- both ascending and descending in one command

scala> empDF.orderBy(empDF("dept").desc, empDF("salary")).show()
+---+-------+------+-----+
| id|   name|salary| dept|
+---+-------+------+-----+
|  2|  Peter|  1200|SALES|
|  8|  Steve| 10000|SALES|
|  4|   Adam|  2000|   IT|
|  5|  Steve|  2500|   IT|
|  6|  Brian|  2700|   IT|
|  1|   Mark|  1000|   HR|
|  3|  Henry|  1500|   HR|
|  7|Michael|  3000|   HR|
|  9|  Peter|  7000|   HR|
| 10|    Dan|  6000|   BS|
+---+-------+------+-----+

STEP-9: 

Writing the SQL like queries

-- register the dataframe as a temporary table
-- once it is done, it can be used as a table

scala> empDF.registerTempTable("employee")
scala> val mid_sal = sqlContext.sql("select name, dept, salary from employee where salary >= 2000 and salary <= 5000")

mid_sal: org.apache.spark.sql.DataFrame = [name: string, dept: string]
scala> mid_sal.show()
+-------+----+
|   name|dept|
+-------+----+
|   Adam|  IT|
|  Steve|  IT|
|  Brian|  IT|
|Michael|  HR|
+-------+----+

scala> sqlContext.sql("select name, dept, salary from employee where salary >= 2000 and salary <= 5000").show()
+-------+----+------+
|   name|dept|salary|
+-------+----+------+
|   Adam|  IT|  2000|
|  Steve|  IT|  2500|
|  Brian|  IT|  2700|
|Michael|  HR|  3000|
+-------+----+------+


-- reading a table in hive from spark
-- there is an existing table DEPT in the database RAJESHK
-- if it is required to be read into the spark, following can be done

scala> val deptDF = sqlContext.sql("select * from rajeshk.dept");
deptDF: org.apache.spark.sql.DataFrame = [d_id: string, d_name: string]

scala> deptDF.show(truncate=false);
+-----+----------------------+
|d_id |d_name                |
+-----+----------------------+
|HR   |Human Resource        |
|SALES| Sales Department     |
|IT   |Information Technology|
+-----+----------------------+


Specify Schema

scala> val input  = sc.textFile("spark_files/emp.txt")
input: org.apache.spark.rdd.RDD[String] = MapPartitionsRDD[128] at textFile at <console>:35

scala> import org.apache.spark.sql.Row
import org.apache.spark.sql.Row

scala> import org.apache.spark.sql.types._
import org.apache.spark.sql.types._

scala> val schema = StructType(Array(StructField("id", IntegerType, true),StructField("name", StringType, true),StructField("salary", IntegerType, true),StructField("dept", StringType, true)))
schema: org.apache.spark.sql.types.StructType = StructType(StructField(id,IntegerType,true), StructField(name,StringType,true), StructField(salary,IntegerType,true), StructField(dept,StringType,true))

scala> val rowrdd = input.map(line => line.split(",")).map(x => Row(x(0).toInt, x(1), x(2).toInt, x(3)))
rowrdd: org.apache.spark.rdd.RDD[org.apache.spark.sql.Row] = MapPartitionsRDD[130] at map at <console>:41

scala> val empDF = sqlContext.createDataFrame(rowrdd, schema)
empDF: org.apache.spark.sql.DataFrame = [id: int, name: string, salary: int, dept: string]

scala> empDF.show()
+---+-------+------+-----+
| id|   name|salary| dept|
+---+-------+------+-----+
|  1|   Mark|  1000|   HR|
|  2|  Peter|  1200|SALES|
|  3|  Henry|  1500|   HR|
|  4|   Adam|  2000|   IT|
|  5|  Steve|  2500|   IT|
|  6|  Brian|  2700|   IT|
|  7|Michael|  3000|   HR|
|  8|  Steve| 10000|SALES|
|  9|  Peter|  7000|   HR|
| 10|    Dan|  6000|   BS|
+---+-------+------+-----+



Create dataframe using JSON file

people.json:
{"name":"Michael"}
{"name":"David","age":12}
{"name":"Steve","age":14}
{"name":"James","age":18}
{"name":"Mark","age":30}
{"name":"Sachin","age":35}
{"name":"Rahul","age":24}
{"name":"Stephen","age":25}
{"name":"Brian","age":26}
{"name":"Chris","age":28}
{"name":"Kevin","age":51}
{"name":"Andrew","age":52}
{"name":"Joss","age":78}
{"name":"Henry","age":34}
{"name":"Aaron"}

scala> val peopleDF = sqlContext.read.json("spark_files/people.json")
peopleDF: org.apache.spark.sql.DataFrame = [age: bigint, name: string]

scala> peopleDF.show()
+----+-------+
| age|   name|
+----+-------+
|null|Michael|
|  12|  David|
|  14|  Steve|
|  18|  James|
|  30|   Mark|
|  35| Sachin|
|  24|  Rahul|
|  25|Stephen|
|  26|  Brian|
|  28|  Chris|
|  51|  Kevin|
|  52| Andrew|
|  78|   Joss|
|  34|  Henry|
|null|  Aaron|
+----+-------+

 

@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@ 
GRAPHX


collection of vertices and edges
 
undirected graph : has no directions between vertices

directed graph: has a direction from one vertex to the other

cyclic graph: all vertices connected and make a loop

acyclic graph: no loop 

disconnected graph: some vertices are not connected 


Vertex can exist with edge
Edge cannot exist without vertex


scala> import org.apache.spark.graphx._
import org.apache.spark.graphx._

scala> import org.apache.spark.rdd.RDD
import org.apache.spark.rdd.RDD

scala> val vertexArray = Array(
     |     (1L, ("Aaron", 28)),
     |     (2L, ("Michael", 27)),
     |     (3L, ("Joss", 66)),
     |     (4L, ("Edward", 55)),
     |     (5L, ("James", 34)),
     |     (6L, ("David", 58)))
vertexArray: Array[(Long, (String, Int))] = Array((1,(Aaron,28)), (2,(Michael,27)), (3,(Joss,66)), (4,(Edward,55)), (5,(James,34)), (6,(David,58)))

scala> val edgeArray = Array(
     |     Edge(2L, 1L, 7),
     |     Edge(2L, 4L, 2),
     |     Edge(3L, 2L, 4),
     |     Edge(3L, 6L, 3),
     |     Edge(4L, 1L, 1),
     |     Edge(5L, 2L, 2),
     |     Edge(5L, 3L, 8),
     |     Edge(5L, 6L, 3)
     |     )
edgeArray: Array[org.apache.spark.graphx.Edge[Int]] = Array(Edge(2,1,7), Edge(2,4,2), Edge(3,2,4), Edge(3,6,3), Edge(4,1,1), Edge(5,2,2), Edge(5,3,8), Edge(5,6,3))

scala> val vertexRDD = sc.parallelize(vertexArray)
vertexRDD: org.apache.spark.rdd.RDD[(Long, (String, Int))] = ParallelCollectionRDD[145] at parallelize at <console>:45

scala> val edgeRDD = sc.parallelize(edgeArray)
edgeRDD: org.apache.spark.rdd.RDD[org.apache.spark.graphx.Edge[Int]] = ParallelCollectionRDD[146] at parallelize at <console>:45

scala> val graph = Graph(vertexRDD, edgeRDD)
graph: org.apache.spark.graphx.Graph[(String, Int),Int] = org.apache.spark.graphx.impl.GraphImpl@351c1d2b

scala> graph.vertices.collect().foreach(println)
(1,(Aaron,28))                                                                  
(2,(Michael,27))
(3,(Joss,66))
(4,(Edward,55))
(5,(James,34))
(6,(David,58))

scala> graph.edges.collect().foreach(println)
Edge(2,1,7)
Edge(2,4,2)
Edge(3,2,4)
Edge(3,6,3)
Edge(4,1,1)
Edge(5,2,2)
Edge(5,3,8)
Edge(5,6,3)


Reverse of a graph

-- changing the direction of the edges in the graph
-- the number of vertices are still same
-- direction of edge is reversed
-- source and destination will get swapped

scala> val rgraph = graph.reverse
rgraph: org.apache.spark.graphx.Graph[(String, Int),Int] = org.apache.spark.graphx.impl.GraphImpl@24d5a11a

scala> rgraph.vertices.collect().foreach(println)
(1,(Aaron,28))
(2,(Michael,27))
(3,(Joss,66))
(4,(Edward,55))
(5,(James,34))
(6,(David,58))

scala> rgraph.edges.collect().foreach(println)
Edge(1,2,7)
Edge(4,2,2)
Edge(2,3,4)
Edge(6,3,3)
Edge(1,4,1)
Edge(2,5,2)
Edge(3,5,8)
Edge(6,5,3)

-- ###########################################################################################################################

Sub Graph

-- It's a part of a graph which satisfies a filter condition
-- It removes the vertices and edges that fail the condition

scala> val sgraph = graph.subgraph(vpred = (id, attr) => attr._2 > 35)
sgraph: org.apache.spark.graphx.Graph[(String, Int),Int] = org.apache.spark.graphx.impl.GraphImpl@967b0a2

scala> sgraph.vertices.collect().foreach(println)
(3,(Joss,66))
(4,(Edward,55))
(6,(David,58))

scala> sgraph.edges.collect().foreach(println)
Edge(3,6,3)


sub graph on source id > 3
===========================
-- remove all the edges starting from vertexes 1, 2, 3 which are going outward
-- note vertex can exist without edge

scala> val sgraph = graph.subgraph(epred = e => e.srcId > 3)
sgraph: org.apache.spark.graphx.Graph[(String, Int),Int] = org.apache.spark.graphx.impl.GraphImpl@6670d46e

scala> sgraph.vertices.collect().foreach(println)
(1,(Aaron,28))                                                                  
(2,(Michael,27))
(3,(Joss,66))
(4,(Edward,55))
(5,(James,34))
(6,(David,58))

scala> sgraph.edges.collect().foreach(println)
Edge(4,1,1)
Edge(5,2,2)
Edge(5,3,8)
Edge(5,6,3)


sub graph on destination id > 3
================================
-- remove all incoming edges on 1 2 and 3
-- all edges ending on 1 2 3 will be gone

scala> val sgraph = graph.subgraph(epred = e => e.dstId > 3)
sgraph: org.apache.spark.graphx.Graph[(String, Int),Int] = org.apache.spark.graphx.impl.GraphImpl@78513ad9

scala> sgraph.vertices.collect().foreach(println)
(1,(Aaron,28))
(2,(Michael,27))
(3,(Joss,66))
(4,(Edward,55))
(5,(James,34))
(6,(David,58))

scala> sgraph.edges.collect().foreach(println)
Edge(2,4,2)
Edge(3,6,3)
Edge(5,6,3)


sub graph on edge attribute id > 3
==================================
-- remove all the edges where the weightage is <=3

scala> val sgraph = graph.subgraph(epred = e => e.attr > 3)
sgraph: org.apache.spark.graphx.Graph[(String, Int),Int] = org.apache.spark.graphx.impl.GraphImpl@470467b0

scala> sgraph.vertices.collect().foreach(println)
(1,(Aaron,28))
(2,(Michael,27))
(3,(Joss,66))
(4,(Edward,55))
(5,(James,34))
(6,(David,58))

scala> sgraph.edges.collect().foreach(println)
Edge(2,1,7)
Edge(3,2,4)
Edge(5,3,8)



Machine Learning
=================

ML is the ability for the computer to learn by themselves so that someone need not program explicitly
Two types of learning by computer
- Supervised Learning	: We have some experience and knowledge about the data.
                          We have a supervisor who can teach to gain the knowledge so that it can be applied going forward 
- Unsupervised Learning	: Need to learn on own and no supervisor.


Recommendation can be built using machine learning

Linear Regression Algorithm
- Used for predictions
- System will try to find the equation of the line like y = mx + c => x is independent variable, y is dependent variable. c is y intercept and m is slope
- uses this equation to predict the new values of y based on value for x

k-means Clustering Algorithm
- certain data points are distributed
- creates the cluster out of the various data points
- creates the k number of clusters.
- ITER:1 system will take k random points into the plane and based on these k points, it takes the distance of each of the points from these k points.
         Centroid of the cluster are the k random points.
		 based on the nearest distance it creates a cluster
- ITER:2 Centroid will be recalculated within the cluster again based on distance within the cluster to each of the points.
- ITER:n location of centroid keeps on changing until we get the location where the centroid is no longer moving.

How ML Algo works:
- ingest the training data into algorithm and it gives certain model
- with the test data, test the model on how it works 
- feed in the real data into the model and predict the final outcome.


KMeans hands on

STEP-1: Load the required libraries

scala> import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.linalg.Vectors

scala> import org.apache.spark.mllib.clustering.KMeans
import org.apache.spark.mllib.clustering.KMeans


STEP-2: Load the data file from HDFS

scala> val trainingFile = sc.textFile("spark_files/kmeans_data.txt")
trainingFile: org.apache.spark.rdd.RDD[String] = MapPartitionsRDD[1] at textFile at <console>:29

scala> trainingFile.collect().foreach(println)
0 0 0
.1 .1 .1
.2 .2 .2
9 9 9
9.1 9.1 9.1
9.2 9.2 9.2

STEP-3: Parse the Data and keep it ready for creating model

scala> val parsedData = trainingFile.map(s => Vectors.dense(s.split(" ").map(x => x.toDouble)))
parsedData: org.apache.spark.rdd.RDD[org.apache.spark.mllib.linalg.Vector] = MapPartitionsRDD[2] at map at <console>:31


STEP-4: Specify number of clusters and iterations

scala> val numClusters = 2
numClusters: Int = 2

scala> val numIterations = 10
numIterations: Int = 10


STEP-5: Prepare the Model

scala> val kMeansModel = KMeans.train(parsedData, numClusters, numIterations)
kMeansModel: org.apache.spark.mllib.clustering.KMeansModel = org.apache.spark.mllib.clustering.KMeansModel@6dbdfa38

-- check the cluster centers

scala> kMeansModel.clusterCenters
res2: Array[org.apache.spark.mllib.linalg.Vector] = Array([0.1,0.1,0.1], [9.099999999999998,9.099999999999998,9.099999999999998])


STEP-6: Load the test data

scala> val testFile = sc.textFile("spark_files/kmeans_test_data.txt")
testFile: org.apache.spark.rdd.RDD[String] = MapPartitionsRDD[33] at textFile at <console>:29

scala> testFile.collect().foreach(println)
7 7 7
3.2 3.2 3.2
5.1 5.1 5.1
8 8 8
3.1 3.2 3.5


STEP-7: Test the model

scala> val parsedTestData = testFile.map(s => Vectors.dense(s.split(" ").map(x => x.toDouble)))
parsedTestData: org.apache.spark.rdd.RDD[org.apache.spark.mllib.linalg.Vector] = MapPartitionsRDD[34] at map at <console>:31

scala> val result = kMeansModel.predict(parsedTestData)
result: org.apache.spark.rdd.RDD[Int] = MapPartitionsRDD[35] at map at KMeansModel.scala:70

scala> result.collect()
res4: Array[Int] = Array(1, 0, 1, 1, 0)


STEP-8: Real time predictions

scala> val v = Vectors.dense(10, 10, 10)
v: org.apache.spark.mllib.linalg.Vector = [10.0,10.0,10.0]

scala> val result = kMeansModel.predict(v)
result: Int = 1

scala> val v = Vectors.dense(.1, 8, 4.2)
v: org.apache.spark.mllib.linalg.Vector = [0.1,8.0,4.2]

scala> val result = kMeansModel.predict(v)
result: Int = 0

scala> val v = Vectors.dense(0, 5, 10)
v: org.apache.spark.mllib.linalg.Vector = [0.0,5.0,10.0]

scala> val result = kMeansModel.predict(v)
result: Int = 1

