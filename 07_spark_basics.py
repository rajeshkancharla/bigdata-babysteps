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

