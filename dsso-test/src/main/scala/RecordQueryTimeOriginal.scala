import QueryTimeRecorder.ImdbDataGenerator
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.execution.ExplainMode

import scala.concurrent.duration.NANOSECONDS
import scala.io.Source

object RecordQueryTimeOriginal {
  def main(args: Array[String]): Unit = {

    val dataDir = args(0)
      val queryDir = args(1)
      val qId = args(2).toInt
      val resDir = args(3)
      val queries = Source.fromFile(queryDir).getLines().toArray
      val query = queries(qId)
      val spark = SparkSession.builder()
      //      .master("local")
      .appName(s"RecordQueryTime_${qId}_original")
      .getOrCreate()

      val sc = spark.sparkContext
      sc.setLogLevel("ERROR")
      val dataGenerator = new ImdbDataGenerator(dataDir)
      dataGenerator.createTable
      val app = spark.sql(query)
      val start1 = System.nanoTime()
      val start2 = System.nanoTime()
      app.write
      .mode("overwrite")
      .format("noop")
      .save()
      val end = System.nanoTime()
      val t1 = NANOSECONDS.toMillis(end - start1).toString
      val t2 = NANOSECONDS.toMillis(end - start2).toString
      val plan = app.queryExecution.explainString(ExplainMode.fromString("formatted"))
      println(s"$qId, org, $t1, $t2")
      println(plan)
      val df = spark.createDataFrame(Seq((qId.toString, "org", t1, t2, plan, query)))
      df.write.mode("append").format("csv").save(resDir)
      sc.stop()
  }
}


