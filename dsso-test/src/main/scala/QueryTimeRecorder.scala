import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.types.{IntegerType, StringType, StructType}
import org.apache.spark.sql.execution.ExplainMode
import org.apache.spark.sql.functions._

import java.io.File
import scala.concurrent.duration.NANOSECONDS
import scala.io.Source

object QueryTimeRecorder {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      //      .master("local")
      .appName("QueryTimeRecorder")
      .getOrCreate()
    val sc = spark.sparkContext
    sc.setLogLevel("ERROR")
    val dataDir = args(0)
    val queryDir = args(1)
    val resDir = args(2)
    val dataGenerator = new ImdbDataGenerator(dataDir)
    //    a.testDf
    //    val queries = getListOfFiles(queryDir)
    val queries = getQueries(queryDir)
    var res = new Array[(String, String, String, String, String)](0)
    for (q <- queries) {
      spark.catalog.clearCache()
      dataGenerator.createView
      //      val queryFile = Source.fromFile(q)
      //      val query = queryFile.mkString
      val query = q
      println(q.toString)
      val plan = spark.sql(query).queryExecution.explainString(ExplainMode.fromString("formatted"))
      val plan2 = spark.sql(query).queryExecution.executedPlan
      val start = System.nanoTime()
      spark.sql(query).write
        .mode("overwrite")
        .format("noop")
        .save()
      val end = System.nanoTime()
      val t = NANOSECONDS.toMillis(end - start).toString
      res = res :+ ((q.toString, query, plan2.toString, plan, t))
    }
    spark.createDataFrame(res.toSeq).coalesce(1).write.csv(resDir)
    sc.stop()
  }

  def getListOfFiles(dir: String): List[File] = {
    val d = new File(dir)
    if (d.exists && d.isDirectory) {
      d.listFiles.filter(_.isFile).toList
    } else {
      List[File]()
    }
  }

  def getQueries(dir: String): Array[String] = {
    val d = new File(dir)
    val files = d.listFiles.filter(_.isFile).toList.filter(x => x.getName.contains(".sql"))
    println(files)
    var queries = new Array[String](0)
    for (file <- files) {
      for (line <- Source.fromFile(file).getLines) {
        queries = queries :+ line
      }
    }
    println(s"== found ${queries.length} queries")
    queries
  }
}



