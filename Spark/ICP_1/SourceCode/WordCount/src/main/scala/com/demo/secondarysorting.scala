package com.demo

import org.apache.spark.{ SparkConf, SparkContext }
import org.apache.spark.rdd.RDD

object secondarysorting {
  def main(args: Array[String]) {

    System.setProperty("hadoop.home.dir","C:\\winutils");
    val conf = new SparkConf().setAppName("SecondarySort").setMaster("local[*]")
    val sc = new SparkContext(conf)
    val output = "sort/output"

    val personRDD = sc.textFile("input2.txt")
    val pairsRDD = personRDD.map(_.split(",")).map { k => ((k(0),k(1)), (k(2),k(3))) }
    println("Pairs RDD")
    pairsRDD.foreach{
      println
    }

    val numReducers = 2;
    val listRDD = pairsRDD.groupByKey(numReducers).mapValues(iter => iter.toList.sortBy(r => r))
    println("List RDD")
    listRDD.foreach{
      println
    }

    val resultRDD = listRDD.flatMap {
      case (label, list) => {
        list.map((label, _))
      }
    }
    resultRDD.saveAsTextFile(output)
    sc.stop()
  }


}
