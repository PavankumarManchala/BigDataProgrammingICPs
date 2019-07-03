package com.demo

import org.apache.spark._
import org.apache.spark.{SparkConf, SparkContext}

object trnsactns {
  def main(args: Array[String]): Unit = {
    System.setProperty("hadoop.home.dir", "C:\\winutils");
    val sparkConf = new SparkConf().setAppName("SparkTransformationsAndActions").setMaster("local[*]")
    val sc = new SparkContext(sparkConf)
    val nums = sc.parallelize(Array(1,2,3,4,5,6,7,8,9))
    println("nums using parallelize")
    nums.foreach(println)

    val squares = nums.map(x => x * x)
    println("squares using map")
    squares.foreach(println)

    val even = squares.filter(x => x % 2 == 0)
    println("even using filter")
    even.foreach(println)

    val result = nums.flatMap(x => Array.range(0, x))
    println("nums flatmap")
    result.foreach(println)

    println("nums count")
    println(nums.count())
    println("nums addition")
    println(nums.reduce((x,y)=>x+y))
    println("nums collect")
    println(nums.collect())


  }

}
