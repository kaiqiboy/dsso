import QueryTimeRecorder.ImdbDataGenerator
import org.apache.spark.sql.SparkSession

import scala.concurrent.duration.NANOSECONDS
import scala.io.Source

object TestOriginal {
  def main(args: Array[String]): Unit = {

    val dataDir = args(0)
      val queryDir = args(1)
      val qId = args(2).toInt
      val data = args(3)
      val queries = Source.fromFile(queryDir).getLines().toArray
      val query = if (data == "stack" || data == "tpch") Source.fromFile(queryDir).getLines.map(x => x.stripMargin).mkString(" ")
      else if (data == "imdb") queries(qId)
        else throw new Exception("choose data from imdb or stack")
          val spark = SparkSession.builder()
            //      .master("local")
            .appName(s"RecordQueryTime_${qId}_original")
            .getOrCreate()

            val sc = spark.sparkContext
            sc.setLogLevel("ERROR")
            val dataGenerator = if (data == "imdb") new ImdbDataGenerator(dataDir)
            else if (data == "stack") new StackDataGenerator(dataDir)
              else if (data == "tpch") new TpchDataGenerator(dataDir)
                else throw new Exception("choose data from imdb or stack or tpch")
                  dataGenerator.createTable
                    val app = spark.sql(query)
                    val start1 = System.nanoTime()
                    app.write
                    .mode("overwrite")
                    .format("noop")
                    .save()
                    val end = System.nanoTime()
                    val t1 = NANOSECONDS.toMillis(end - start1).toString
                    val planId = if (data == "stack" || data == "tpch") queryDir else qId
                    println(s"query $planId: $t1")
                    sc.stop()
  }
}
