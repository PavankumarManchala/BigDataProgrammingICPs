import sys
import os

os.environ["SPARK_HOME"] = "C:\\spark-2.4.3-bin-hadoop2.7"
os.environ["HADOOP_HOME"]="C:\\winutils"

from pyspark import SparkContext
from pyspark.streaming import StreamingContext

def main():
    sc = SparkContext(appName="PysparkStreaming")
    ssc = StreamingContext(sc, 5)   #Streaming will execute in each 5 seconds

    lines = ssc.socketTextStream("localhost", 5000)
    words = lines.flatMap(lambda line: line.split(" "))
    counts = words.map(lambda word: (len(word), word))\
        .reduceByKey(lambda x, y: x + " " + y)
    counts.pprint()

    ssc.start()
    ssc.awaitTermination()

if __name__ == "__main__":
    main()