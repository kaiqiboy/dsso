import org.apache.spark.sql.{DataFrame, SparkSession}

abstract class DataGenerator {
  val spark: SparkSession
  val dfs: Array[DataFrame]
  val dfNameArr: Array[(String, String)]

  def testDf: Unit

  def createTable: Unit

  def createView: Unit
}

