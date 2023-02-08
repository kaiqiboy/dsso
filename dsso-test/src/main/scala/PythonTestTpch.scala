import org.apache.spark.sql.SparkSession
import scala.concurrent.duration.NANOSECONDS
import scala.io.Source

object PythonTestTpch {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      //      .master("local")
      .appName("pythontest")
      .getOrCreate()
      val sc = spark.sparkContext
      sc.setLogLevel("ERROR")

      spark.catalog.pruneSimilar = "agg"
      val dataDir = args(0)
      val queryDir = args(1)

      val query = Source.fromFile(queryDir).getLines.map(x => x.stripMargin).filter(!_.contains("--")).mkString(" ")

      val dataGenerator = new TpchDataGenerator(dataDir)
      dataGenerator.createTable
      val app = spark.sql(query)
      val start1 = System.nanoTime()
      app.write
      .mode("overwrite")
      .format("noop")
      .save()
      val end = System.nanoTime()
      val t1 = NANOSECONDS.toMillis(end - start1).toString
      println(s"query $queryDir: $t1")
      sc.stop()
  }
}
