from pyspark import SparkContext, SparkConf
from pypark.streaming import StreamingContext

conf = SparkConf.setAppName("Streaming Word Count").setMaster("yarn-client")
sc = SparkContext(conf = conf)
ssc = StreamingContext(sc, 15) -- to poll every 15 seconds

lines = ssc.socketTextStream("gw01.itversity.com", 19999)
words = lines.flatMap(lambda line: line.split(" "))
wordTuples = words.map(lambda word: (word,1)
wordCount = wordTuples.reduceByKey(lambda x, y: x + y)
wordCount.print()

ssc.start
ssc.awaitTermination()
