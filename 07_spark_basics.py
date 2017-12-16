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

