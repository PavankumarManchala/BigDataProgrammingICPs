import org.apache.spark._
import org.apache.spark.rdd.RDD
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.SparkSession
import org.apache.log4j._
import org.graphframes._

object icp6 {
  def main(args: Array[String]) {
    System.setProperty("hadoop.home.dir", "C:\\winutils")

    Logger.getLogger("org").setLevel(Level.ERROR)
    Logger.getLogger("akka").setLevel(Level.ERROR)

    val spark = SparkSession
      .builder()
      .appName("Spark SQL basic example")
      .config("spark.master", "local")
      .getOrCreate()
    val input = spark.read.format("csv").option("header", "true").load("C:\\Courses_Masters\\BigDataProgramming\\Spark\\ICP5\\ICP5\\201508_station_data.csv")

    //input.show()
    val input1 = input.select("name","landmark","dockcount").withColumnRenamed("name","id")
    //input1.show()

    val output = spark.read.format("csv").option("header", "true").load("C:\\Courses_Masters\\BigDataProgramming\\Spark\\ICP5\\ICP5\\201508_trip_data.csv")
    //output.show()
    val output1 = output.select("Start Station","End Station","Duration").withColumnRenamed("Start Station","src").withColumnRenamed("End Station","dst").withColumnRenamed("Duration","relationship")
    //output1.show()


    val g = GraphFrame(input1,output1)
    g.vertices.show()
    g.edges.show()

//    println("Triangle count")
    val results = g.triangleCount.run()
    results.show()
    results.select("id", "count").show()

//    println("Shortest path")
    val sp = g.shortestPaths.landmarks(Seq("Japantown","Cowper at University")).run()
    sp.show()

//    println("Page rank")
    val pr = g.pageRank.resetProbability(0.15).tol(0.1).run()
    pr.vertices.show()
    pr.edges.show()

    g.vertices.write.csv("C:\\Courses_Masters\\BigDataProgramming\\Spark\\ICP5\\ICP5\\Graphframe2\\ver")
    g.edges.write.csv("C:\\Courses_Masters\\BigDataProgramming\\Spark\\ICP5\\ICP5\\Graphframe2\\edge")

    val BFS = g.bfs.fromExpr("id = 'Japantown'").toExpr("id = 'San Pedro Square").run()
    BFS.show()

    val lp = g.labelPropagation.maxIter(5).run()
    lp.select("id", "label").show()

  }
}