import QueryTimeRecorder.ImdbDataGenerator
import org.apache.spark.sql.SparkSession

import scala.concurrent.duration.NANOSECONDS
import scala.io.Source
import java.io._

object RecordQueryTime {
  def main(args: Array[String]): Unit = {

    val dataDir = args(0)
      val queryDir = args(1)
      val planId = args(2).toInt
      val resDir = args(3)
      val infoDir = if (args(4).endsWith("/")) args(4) else args(4) + "/"
      val qId = queryDir.split("/")(6)+"-"+queryDir.split("/").last.split("\\.").head
      val query = Source.fromFile(queryDir).getLines().next()
      val spark = SparkSession.builder()
      //      .master("local")
      .appName(s"RecordQueryTime_${qId}_${planId}")
      .getOrCreate()
      spark.catalog.planIndex = planId

      val sc = spark.sparkContext
      sc.setLogLevel("ERROR")
      val dataGenerator = new ImdbDataGenerator(dataDir)
      dataGenerator.createTable
      val app = spark.sql(query)
      val start1 = System.nanoTime()
      app.queryExecution.generateCandidatePlans
      val start2 = System.nanoTime()

      val plan = app.queryExecution.selectedPlanString
      val info = Seq(qId, planId.toString, s"${'"'}$plan${'"'}").mkString(",")
      val pw = new PrintWriter(new File(s"${infoDir}${qId}-${planId}.csv"))
      pw.write(info)
      pw.close()

      app.write
      .mode("overwrite")
      .format("noop")
      .save()
      val end = System.nanoTime()
      val t1 = NANOSECONDS.toMillis(end - start1).toString
      val t2 = NANOSECONDS.toMillis(end - start2).toString
      println(s"$qId, $planId, $t1, $t2")
      println(plan)
      val df = spark.createDataFrame(Seq((qId.toString, planId.toString, t1, t2, plan, query)))
      df.write.mode("append").format("csv").save(resDir)
      new File("${infoDir}${qId}-${planId}.csv").delete()
      sc.stop()
  }
}

