import org.apache.spark.sql.{DataFrame, SparkSession}

import java.io.File
import scala.reflect.io.Directory

abstract class DataGenerator {
  val spark: SparkSession
  val dfs: Array[DataFrame]
  val dfNameArr: Array[(String, String)]

  def testDf: Unit

  def createTable: Unit

  def createView: Unit

  def clean: Unit = {
    val directory = new Directory(new File("./spark-warehouse"))
    directory.deleteRecursively()
  }
}

