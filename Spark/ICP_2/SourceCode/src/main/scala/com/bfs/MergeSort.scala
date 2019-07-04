package com.bfs
import org.apache.spark._
import org.apache.log4j.{Level,Logger}

object MergeSort {
  def main(args: Array[String]): Unit = {

    Logger.getLogger("org").setLevel(Level.ERROR)
    Logger.getLogger("akka").setLevel(Level.ERROR)
    System.setProperty("hadoop.home.dir","C:\\winutils" );

    val conf = new SparkConf().setAppName("MergeSort").setMaster("local[*]")

    val sc = new SparkContext(conf)

    val input = sc.textFile("input.txt")

    val nums = input.flatMap(line => line.split(" "))
    nums.collect

    val nums2 = nums.sortBy(x=>x).collect
    //nums.saveAsTextFile("output")
   // println(sorts)
    for (elem <- nums2) {
      println(elem)
    }
  }

}

