import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object GetTableStats {
  case class TableInfo(name: String,
                       length: Int,
                       size: Long,
                       columns: Map[String, ColInfo])

  case class ColInfo(numerical: Int, // 1 for true and 0 for false
                     minValue: Double,
                     maxValue: Double,
                     nulls: Int,
                     distinct: Int)

  def main(args: Array[String]): Unit = {
    val dataDir = args(0)
    val resDir = args(1)
    val dataset = args(2)
    val spark = SparkSession.builder()
      //.master("local")
      .appName("GetTableStats")
      .getOrCreate()
    val sc = spark.sparkContext
    sc.setLogLevel("ERROR")
    val dataGenerator = if (dataset == "imdb") new ImdbDataGenerator(dataDir)
    else if (dataset == "tpch") new TpchDataGenerator(dataDir)
    else throw new Exception("unknown dataset")
    val dfs = dataGenerator.dfs
    var tableInfos = new Array[TableInfo](0)
    val names = dataGenerator.dfNameArr.map(_._2)
    for ((df, i) <- dfs.zipWithIndex) {
      val name = names(i)
      df.cache.foreach(_ => ())
      val catalyst_plan = df.queryExecution.logical
      val dfSize = spark.sessionState.executePlan(
        catalyst_plan).optimizedPlan.stats.sizeInBytes.toLong
      val length = df.count().toInt
      val columns = df.columns
      val colInfo = columns.map { col =>
        val numerical = if (df.schema(col).dataType.typeName == "integer") 1 else 0
        val minValue = if (!df.agg(min(col)).collect().head.isNullAt(0) && numerical == 1) df.agg(min(col)).collect().head.getInt(0) else 1
        val maxValue = if (!df.agg(max(col)).collect().head.isNullAt(0) && numerical == 1) df.agg(max(col)).collect().head.getInt(0) else -1
        val nulls = df.select(col).filter(x => x.anyNull).count.toInt
        val distinct = df.select(col).distinct.count.toInt
        val info = ColInfo(numerical, minValue, maxValue, nulls, distinct)
        (col, info)
      }
      val tableInfo = TableInfo(name, length, dfSize, colInfo.toMap)
      tableInfos = tableInfos :+ tableInfo
    }

    import spark.implicits._

    val ds = spark.createDataset(tableInfos).coalesce(1)
    ds.write.json(resDir)
    sc.stop()
  }
}


