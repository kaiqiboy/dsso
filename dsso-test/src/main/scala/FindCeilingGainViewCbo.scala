import org.apache.spark.sql.SparkSession

import java.io._
import scala.concurrent.duration.NANOSECONDS
import scala.io.Source

object FindCeilingGainViewCbo {
  def main(args: Array[String]): Unit = {

    val dataDir = args(0)
    val queryDir = args(1)
    val planId = args(2).toInt
    val resDir = args(3)
    val infoDir = if (args(4).endsWith("/")) args(4) else args(4) + "/"
    val planIds = args(5).split(",").map(_.toInt)

    val spark = SparkSession.builder()
      .appName(s"RecordQueryTimeTpch")
      .getOrCreate()
    spark.catalog.planIndex = planId
    spark.catalog.pruneSimilar = "agg"
    val sc = spark.sparkContext
    sc.setLogLevel("ERROR")
    val dataGenerator = new TpchDataGenerator(dataDir)
    dataGenerator.createView
    val query = Source.fromFile(queryDir).getLines.map(x => x.stripMargin).filter(!_.contains("--")).mkString(" ")
    for (qId <- Range(planIds(0), planIds(1) + 1, 1)) {
      val app = spark.sql(query)
      val start1 = System.nanoTime()
      app.queryExecution.generateCandidatePlans
      println(app.queryExecution.candidateSparkPlans.get.length)
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
    }
    sc.stop()
  }
}

