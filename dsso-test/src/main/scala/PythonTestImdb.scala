import org.apache.spark.sql.SparkSession
import scala.concurrent.duration.NANOSECONDS
import scala.io.Source

object PythonTestImdb {
  def main(args: Array[String]): Unit = {
    val dataDir = args(0)
      val queryDir = args(1)
      val queryId = args(2).toInt
      val spark = SparkSession.builder()
      //      .master("local")
      .appName(s"pythontest-$queryId")
      .getOrCreate()
      val sc = spark.sparkContext
      sc.setLogLevel("ERROR")
      spark.catalog.pruneSimilar = "agg"

      val queries = Source.fromFile(queryDir).getLines().toArray
      val query = queries(queryId)

      val dataGenerator = new ImdbDataGenerator(dataDir)
      dataGenerator.createView
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

