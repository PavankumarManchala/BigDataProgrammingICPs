package com.bfs
import org.apache.spark._
import org.apache.log4j.{Level,Logger}

object testmergesort {
  def main(args: Array[String]): Unit = {

    Logger.getLogger("org").setLevel(Level.ERROR)
    Logger.getLogger("akka").setLevel(Level.ERROR)
    System.setProperty("hadoop.home.dir","C:\\winutils" );

    val conf = new SparkConf().setAppName("MergeSort").setMaster("local[*]")

    val sc = new SparkContext(conf)

    val input = sc.parallelize(Array(1,5,4,42,12,9,2,7,3))

    input.collect
    val nums=input.sortBy(x=>x).collect
    for (elem <- nums) {
      println(elem)
    }


    //val sorts = nums.map(word => (word, 1)).sortByKey()

    //nums.saveAsTextFile("output")
    // println(sorts)
  }

}
