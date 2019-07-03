package com.demo
import org.apache.spark.{SparkConf, SparkContext}
object ta {
  def main(args: Array[String]) {

    System.setProperty("hadoop.home.dir","C:\\winutils");

    val sparkConf = new SparkConf().setAppName("TransformationsActions").setMaster("local[*]")

    val sc=new SparkContext(sparkConf)

    val input =  sc.textFile("input.txt")

    val output = "output"

    val words = input.flatMap(line => line.split(" "))

    words.foreach(f=>println(f))

    val counts = words.map(words => (words, 1)).reduceByKey(_+_,1)

    val wordsList=counts.sortBy(outputLIst=>outputLIst._1,ascending = true)

    wordsList.foreach(outputLIst=>println(outputLIst))

    wordsList.saveAsTextFile(output)

    wordsList.take(15).foreach(outputLIst=>println(outputLIst))

    sc.stop()

  }





}
