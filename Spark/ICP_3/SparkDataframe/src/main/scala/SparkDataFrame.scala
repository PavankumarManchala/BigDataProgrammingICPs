import org.apache.spark._
import org.apache.spark.sql.SparkSession
import org.apache.log4j._
import org.apache.spark.sql.catalyst.expressions.aggregate.Max

object SparkDataFrame {
  def main(args: Array[String]) {
    System.setProperty("hadoop.home.dir", "C:\\winutils")
    val conf = new SparkConf().setMaster("local[2]").setAppName("my app")
    val sc = new SparkContext(conf)
    val spark = SparkSession
      .builder()
      .appName("Spark SQL basic example")
      .config("spark.master", "local")
      .getOrCreate()


    Logger.getLogger("org").setLevel(Level.ERROR)
    Logger.getLogger("akka").setLevel(Level.ERROR)

    import spark.implicits._

    // val df = spark.read.csv("C:\\Users\\Pavankumar Manchala\\Downloads\\Source Code\\SparkDataframe\\survey.csv")
    val df = spark.read
     .format("csv")
     .option("header", "true") //reading the headers
     .option("mode", "DROPMALFORMED")
     .load("C:\\Users\\Pavankumar Manchala\\Downloads\\Source Code\\SparkDataframe\\" +
       "survey.csv")

    df.createOrReplaceTempView("survey")

    df.write.format("csv").save("C:\\Users\\Pavankumar Manchala\\Downloads\\Source Code\\SparkDataframe\\output")

    val DupDF = spark.sql("select COUNT(*),Country,Timestamp from survey GROUP By Timestamp,Country Having COUNT(*) > 1")
    val uniq=df.dropDuplicates().show()
    println("no of duplicate records "+ uniq)
//    DupDF.show()

    val counts=df.count()
    println("total records"+counts)

    val df1 = df.limit(5)
    val df2 = df.limit(10)
    val unionDf = df1.union(df2)

    print("union: orderby country")
    unionDf.orderBy("Country").show()

//    df1.filter("Age>30")
//      .groupBy(df1("state"))
//      .agg(Avg(df1("Age")),Max(df1("Age")))

    val  state= spark.sql("select count(Country),state from survey GROUP BY state ")
    print("state:groupby")
    state.show()

    val Max = spark.sql("select Max(Age) from survey where Age>40")
    Max.show()

    val Avg = spark.sql("select Avg(Age) from survey")
    Avg.show()

    val df3 = df.limit(30)
    val df4 = df.limit(50)

    df3.createOrReplaceTempView("A")
    df4.createOrReplaceTempView("B")

    val joinSQl = spark.sql("select A.state,B.Country FROM A,B where A.Timestamp = " +
      "B.Timestamp")
    print("Join")
    joinSQl.show()

    val df13th = df.take(13).last
    print("13th row "+ df13th)

    def parseLine(line: String) =
    {
      val fields = line.split(",")
      val Timestamp = fields(0).toString
      val Age = fields(1).toString
      val Gender = fields(2)
      val no_empl = fields(9).toString
      (Timestamp,Age,Gender,no_empl)
    }

    val lines = sc.textFile("C:\\Users\\Pavankumar Manchala\\Downloads\\Source Code\\SparkDataframe\\" + "survey.csv")
    val pl = lines.map(parseLine).toDF()

    pl.show()

  }
}