import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.storage.StorageLevel
import org.apache.spark.sql.types.{DoubleType, LongType, StringType, StructType}

import scala.concurrent.duration.NANOSECONDS
import scala.io.Source

case class Customer(
                     c_custkey: Long, // primary key
                     c_name: String,
                     c_address: String,
                     c_nationkey: Long,
                     c_phone: String,
                     c_acctbal: Double,
                     c_mktsegment: String,
                     c_comment: String)


case class Lineitem(
                     l_orderkey: Long, // primary key
                     l_partkey: Long,
                     l_suppkey: Long,
                     l_linenumber: Long, // primary key
                     l_quantity: Double,
                     l_extendedprice: Double,
                     l_discount: Double,
                     l_tax: Double,
                     l_returnflag: String,
                     l_linestatus: String,
                     l_shipdate: String,
                     l_commitdate: String,
                     l_receiptdate: String,
                     l_shipinstruct: String,
                     l_shipmode: String,
                     l_comment: String)


case class Nation(
                   n_nationkey: Long, // primary key
                   n_name: String,
                   n_regionkey: Long,
                   n_comment: String)


case class Order(
                  o_orderkey: Long, // primary key
                  o_custkey: Long,
                  o_orderstatus: String,
                  o_totalprice: Double,
                  o_orderdate: String,
                  o_orderpriority: String,
                  o_clerk: String,
                  o_shippriority: Long,
                  o_comment: String)


case class Part(
                 p_partkey: Long, // primary key
                 p_name: String,
                 p_mfgr: String,
                 p_brand: String,
                 p_type: String,
                 p_size: Long,
                 p_container: String,
                 p_retailprice: Double,
                 p_comment: String)

case class Partsupp(
                     ps_partkey: Long, // primary key
                     ps_suppkey: Long, // primary key
                     ps_availqty: Long,
                     ps_supplycost: Double,
                     ps_comment: String)

case class Region(
                   r_regionkey: Long, // primary key
                   r_name: String,
                   r_comment: String)

case class Supplier(
                     s_suppkey: Long, // primary key
                     s_name: String,
                     s_address: String,
                     s_nationkey: Long,
                     s_phone: String,
                     s_acctbal: Double,
                     s_comment: String)


class TpchDataGenerator(dir: String) extends DataGenerator {

  val inputDir: String = if (dir.endsWith("/")) dir else dir + "/"
  override val spark: SparkSession = SparkSession.builder().getOrCreate()

  import spark.implicits._

  val customer_schema: StructType = new StructType()
    .add("c_custkey", LongType, nullable = false)
    .add("c_name", StringType, nullable = true)
    .add("c_address", StringType, nullable = true)
    .add("c_nationkey", LongType, nullable = true)
    .add("c_phone", StringType, nullable = true)
    .add("c_acctbal", DoubleType, nullable = true)
    .add("c_mktsegment", StringType, nullable = true)
    .add("c_comment", StringType, nullable = true)

  val lineitem_schema: StructType = new StructType()
    .add("l_orderkey", LongType, nullable = false)
    .add("l_partkey", LongType, nullable = true)
    .add("l_suppkey", LongType, nullable = true)
    .add("l_linenumber", LongType, nullable = false)
    .add("l_quantity", DoubleType, nullable = true)
    .add("l_extendedprice", DoubleType, nullable = true)
    .add("l_discount", DoubleType, nullable = true)
    .add("l_tax", DoubleType, nullable = true)
    .add("l_returnflag", StringType, nullable = true)
    .add("l_linestatus", StringType, nullable = true)
    .add("l_shipdate", StringType, nullable = true)
    .add("l_commitdate", StringType, nullable = true)
    .add("l_receiptdate", StringType, nullable = true)
    .add("l_shipinstruct", StringType, nullable = true)
    .add("l_shipmode", StringType, nullable = true)
    .add("l_comment", StringType, nullable = true)

  val nation_schema: StructType = new StructType()
    .add("n_nationkey", LongType, nullable = false)
    .add("n_name", StringType, nullable = true)
    .add("n_regionkey", LongType, nullable = true)
    .add("n_comment", StringType, nullable = true)

  val order_schema: StructType = new StructType()
    .add("o_orderkey", LongType, nullable = false)
    .add("o_custkey", LongType, nullable = true)
    .add("o_orderstatus", StringType, nullable = true)
    .add("o_totalprice", DoubleType, nullable = true)
    .add("o_orderdate", StringType, nullable = true)
    .add("o_orderpriority", StringType, nullable = true)
    .add("o_clerk", StringType, nullable = true)
    .add("o_shippriority", LongType, nullable = true)
    .add("o_comment", StringType, nullable = true)

  val part_schema: StructType = new StructType()
    .add("p_partkey", LongType, nullable = false)
    .add("p_name", StringType, nullable = true)
    .add("p_mfgr", StringType, nullable = true)
    .add("p_brand", StringType, nullable = true)
    .add("p_type", StringType, nullable = true)
    .add("p_size", LongType, nullable = true)
    .add("p_container", StringType, nullable = true)
    .add("p_retailprice", DoubleType, nullable = true)
    .add("p_comment", StringType, nullable = true)

  val partsupp_schema: StructType = new StructType()
    .add("ps_partkey", LongType, nullable = false)
    .add("ps_suppkey", LongType, nullable = false)
    .add("ps_availqty", LongType, nullable = true)
    .add("ps_supplycost", DoubleType, nullable = true)
    .add("ps_comment", StringType, nullable = true)

  val region_schema: StructType = new StructType()
    .add("r_regionkey", LongType, nullable = false)
    .add("r_name", StringType, nullable = false)
    .add("r_comment", StringType, nullable = true)

  val supplier_schema: StructType = new StructType()
    .add("s_suppkey", LongType, nullable = false)
    .add("s_name", StringType, nullable = false)
    .add("s_address", StringType, nullable = true)
    .add("s_nationkey", LongType, nullable = true)
    .add("s_phone", StringType, nullable = true)
    .add("s_acctbal", DoubleType, nullable = true)
    .add("s_comment", StringType, nullable = true)

  val dfMap: Map[String, DataFrame] = Map(
    "customer" -> spark.read.option("delimiter", "|").schema(customer_schema).csv(inputDir + "customer.tbl"),
    "lineitem" -> spark.read.option("delimiter", "|").schema(lineitem_schema).csv(inputDir + "lineitem.tbl"),
    "nation" -> spark.read.option("delimiter", "|").schema(nation_schema).csv(inputDir + "nation.tbl"),
    "region" -> spark.read.option("delimiter", "|").schema(region_schema).csv(inputDir + "region.tbl"),
    "orders" -> spark.read.option("delimiter", "|").schema(order_schema).csv(inputDir + "orders.tbl"),
    "part" -> spark.read.option("delimiter", "|").schema(part_schema).csv(inputDir + "part.tbl"),
    "partsupp" -> spark.read.option("delimiter", "|").schema(partsupp_schema).csv(inputDir + "partsupp.tbl"),
    "supplier" -> spark.read.option("delimiter", "|").schema(supplier_schema).csv(inputDir + "supplier.tbl")
  )

  //  val dfMap: Map[String, DataFrame] = Map(
  //    "customer" -> spark.read.option("delimiter", "|").csv(inputDir + "customer/customer.tbl*").map(p =>
  //      Customer(p.getString(0).toLong, p.getString(1), p.getString(2), p.getString(3).toLong, p.getString(4), p.getString(5).toDouble, p.getString(6), p.getString(7))).toDF(),
  //
  //    "lineitem" -> spark.read.option("delimiter", "|").csv(inputDir + "lineitem/lineitem.tbl*").map(p =>
  //      Lineitem(p.getString(0).toLong, p.getString(1).toLong, p.getString(2).toLong, p.getString(3).toLong, p.getString(4).toDouble, p.getString(5).toDouble, p.getString(6).toDouble, p.getString(7).toDouble, p.getString(8), p.getString(9), p.getString(10), p.getString(11), p.getString(12), p.getString(13), p.getString(14), p.getString(15))).toDF(),
  //
  //    "nation" -> spark.read.option("delimiter", "|").csv(inputDir + "nation/nation.tbl*").map(p =>
  //      Nation(p.getString(0).toLong, p.getString(1), p.getString(2).toLong, p.getString(3))).toDF(),
  //
  //    "region" -> spark.read.option("delimiter", "|").csv(inputDir + "region/region.tbl*").map(p =>
  //      Region(p.getString(0).toLong, p.getString(1), p.getString(2))).toDF(),
  //
  //    "orders" -> spark.read.option("delimiter", "|").csv(inputDir + "orders/orders.tbl*").map(p =>
  //      Order(p.getString(0).toLong, p.getString(1).toLong, p.getString(2), p.getString(3).toDouble, p.getString(4), p.getString(5), p.getString(6), p.getString(7).toLong, p.getString(8))).toDF(),
  //
  //    "part" -> spark.read.option("delimiter", "|").csv(inputDir + "part/part.tbl*").map(p =>
  //      Part(p.getString(0).toLong, p.getString(1), p.getString(2), p.getString(3), p.getString(4), p.getString(5).toLong, p.getString(6), p.getString(7).toDouble, p.getString(8))).toDF(),
  //
  //    "partsupp" -> spark.read.option("delimiter", "|").csv(inputDir + "partsupp/partsupp.tbl*").map(p =>
  //      Partsupp(p.getString(0).toLong, p.getString(1).toLong, p.getString(2).toLong, p.getString(3).toDouble, p.getString(4))).toDF(),
  //
  //    "supplier" -> spark.read.option("delimiter", "|").csv(inputDir + "supplier/supplier.tbl*").map(p =>
  //      Supplier(p.getString(0).toLong, p.getString(1), p.getString(2), p.getString(3).toLong, p.getString(4), p.getString(5).toDouble, p.getString(6))).toDF()
  //  )
  //
  //
  //  val dfMap: Map[String, DataFrame] = Map(
  //
  //    "customer" -> spark.read.textFile(inputDir + "customer/customer.tbl*").map(_.split('|')).map(p =>
  //      Customer(p(0).trim.toLong, p(1).trim, p(2).trim, p(3).trim.toLong, p(4).trim, p(5).trim.toDouble, p(6).trim, p(7).trim)).toDF(),
  //
  //    "lineitem" -> spark.read.textFile(inputDir + "lineitem/lineitem.tbl*").map(_.split('|')).map(p =>
  //      Lineitem(p(0).trim.toLong, p(1).trim.toLong, p(2).trim.toLong, p(3).trim.toLong, p(4).trim.toDouble, p(5).trim.toDouble, p(6).trim.toDouble, p(7).trim.toDouble, p(8).trim, p(9).trim, p(10).trim, p(11).trim, p(12).trim, p(13).trim, p(14).trim, p(15).trim)).toDF(),
  //
  //    "nation" -> spark.read.textFile(inputDir + "nation/nation.tbl*").map(_.split('|')).map(p =>
  //      Nation(p(0).trim.toLong, p(1).trim, p(2).trim.toLong, p(3).trim)).toDF(),
  //
  //    "region" -> spark.read.textFile(inputDir + "region/region.tbl*").map(_.split('|')).map(p =>
  //      Region(p(0).trim.toLong, p(1).trim, p(2).trim)).toDF(),
  //
  //    "orders" -> spark.read.textFile(inputDir + "orders/orders.tbl*").map(_.split('|')).map(p =>
  //      Order(p(0).trim.toLong, p(1).trim.toLong, p(2).trim, p(3).trim.toDouble, p(4).trim, p(5).trim, p(6).trim, p(7).trim.toLong, p(8).trim)).toDF(),
  //
  //    "part" -> spark.read.textFile(inputDir + "part/part.tbl*").map(_.split('|')).map(p =>
  //      Part(p(0).trim.toLong, p(1).trim, p(2).trim, p(3).trim, p(4).trim, p(5).trim.toLong, p(6).trim, p(7).trim.toDouble, p(8).trim)).toDF(),
  //
  //    "partsupp" -> spark.read.textFile(inputDir + "partsupp/partsupp.tbl*").map(_.split('|')).map(p =>
  //      Partsupp(p(0).trim.toLong, p(1).trim.toLong, p(2).trim.toLong, p(3).trim.toDouble, p(4).trim)).toDF(),
  //
  //    "supplier" -> spark.read.textFile(inputDir + "supplier/supplier.tbl*").map(_.split('|')).map(p =>
  //      Supplier(p(0).trim.toLong, p(1).trim, p(2).trim, p(3).trim.toLong, p(4).trim, p(5).trim.toDouble, p(6).trim)).toDF()
  //  )
  //
  //
  //  val dfMap: Map[String, DataFrame] = Map(
  //    "customer" -> spark.read.parquet(inputDir + "customer"),
  //    "nation" -> spark.read.parquet(inputDir + "nation"),
  //    "region" -> spark.read.parquet(inputDir + "region"),
  //    "orders" -> spark.read.parquet(inputDir + "orders"),
  //    "part" -> spark.read.parquet(inputDir + "part"),
  //    "partsupp" -> spark.read.parquet(inputDir + "partsupp"),
  //    "supplier" -> spark.read.parquet(inputDir + "supplier"),
  //    "lineitem" -> spark.read.parquet(inputDir + "lineitem"),
  //  )

  override val dfs: Array[DataFrame] = dfMap.values.toArray
  override val dfNameArr: Array[(String, String)] = dfMap.keys.toArray.map(x => (x, x))

  override def testDf(): Unit = {
    dfMap.foreach { x =>
      println(x._1)
      x._2.printSchema()
      x._2.show(2)
    }
  }

  override def createView(): Unit = {
    dfMap.foreach { x =>
      x._2.createOrReplaceTempView(x._1)
    }
  }

  def writeParquet(): Unit = {
    dfMap.values.foreach(x => x.persist(StorageLevel.MEMORY_AND_DISK))
    for ((name, df) <- dfMap) {
      println(name)
      df.repartition(256).write.parquet(dir + "parquet/" + name)
      df.unpersist()
    }
  }
}

object TpchWriteParquet {
  def main(args: Array[String]): Unit = {
    val start = System.nanoTime()
    val inputDir = args(0)
    val queryDir = args(1)
    val spark = SparkSession.builder()
      //      .master("local[*]")
      .appName("TpchDataGeneratorTest")
      .getOrCreate()
    val sc = spark.sparkContext
    sc.setLogLevel("ERROR")
    import spark.implicits._
    println("customer")
    spark.read.textFile(inputDir + "customer/customer.tbl*").map(_.split('|')).map(p =>
      Customer(p(0).trim.toLong, p(1).trim, p(2).trim, p(3).trim.toLong, p(4).trim, p(5).trim.toDouble, p(6).trim, p(7).trim)).toDF()
      .write.parquet(inputDir + "parquet/customer")
    spark.catalog.clearCache()

    println("lineitem")
    spark.read.textFile(inputDir + "lineitem/lineitem.tbl*").map(_.split('|')).map(p =>
      Lineitem(p(0).trim.toLong, p(1).trim.toLong, p(2).trim.toLong, p(3).trim.toLong, p(4).trim.toDouble, p(5).trim.toDouble, p(6).trim.toDouble, p(7).trim.toDouble, p(8).trim, p(9).trim, p(10).trim, p(11).trim, p(12).trim, p(13).trim, p(14).trim, p(15).trim)).toDF()
      .write.parquet(inputDir + "parquet/lineitem")
    spark.catalog.clearCache()

    println("nation")
    spark.read.textFile(inputDir + "nation/nation.tbl*").map(_.split('|')).map(p =>
      Nation(p(0).trim.toLong, p(1).trim, p(2).trim.toLong, p(3).trim)).toDF()
      .write.parquet(inputDir + "parquet/nation")
    spark.catalog.clearCache()

    println("region")
    spark.read.textFile(inputDir + "region/region.tbl*").map(_.split('|')).map(p =>
      Region(p(0).trim.toLong, p(1).trim, p(2).trim)).toDF()
      .write.parquet(inputDir + "parquet/region")
    spark.catalog.clearCache()

    println("orders")
    spark.read.textFile(inputDir + "orders/orders.tbl*").map(_.split('|')).map(p =>
      Order(p(0).trim.toLong, p(1).trim.toLong, p(2).trim, p(3).trim.toDouble, p(4).trim, p(5).trim, p(6).trim, p(7).trim.toLong, p(8).trim)).toDF()
      .write.parquet(inputDir + "parquet/orders")
    spark.catalog.clearCache()

    println("part")
    spark.read.textFile(inputDir + "part/part.tbl*").map(_.split('|')).map(p =>
      Part(p(0).trim.toLong, p(1).trim, p(2).trim, p(3).trim, p(4).trim, p(5).trim.toLong, p(6).trim, p(7).trim.toDouble, p(8).trim)).toDF()
      .write.parquet(inputDir + "parquet/part")
    spark.catalog.clearCache()

    println("partsupp")
    spark.read.textFile(inputDir + "partsupp/partsupp.tbl*").map(_.split('|')).map(p =>
      Partsupp(p(0).trim.toLong, p(1).trim.toLong, p(2).trim.toLong, p(3).trim.toDouble, p(4).trim)).toDF()
      .write.parquet(inputDir + "parquet/partsupp")
    spark.catalog.clearCache()

    println("supplier")
    spark.read.textFile(inputDir + "supplier/supplier.tbl*").map(_.split('|')).map(p =>
      Supplier(p(0).trim.toLong, p(1).trim, p(2).trim, p(3).trim.toLong, p(4).trim, p(5).trim.toDouble, p(6).trim)).toDF()
      .write.parquet(inputDir + "parquet/supplier")

  }
}

object TpchDataGeneratorTest {
  def main(args: Array[String]): Unit = {
    val start = System.nanoTime()
    val dataDir = args(0)
    val queryDir = args(1)
    val spark = SparkSession.builder()
      //      .master("local[*]")
      .appName("TpchDataGeneratorTest")
      .getOrCreate()
    val sc = spark.sparkContext
    sc.setLogLevel("ERROR")
    println(queryDir)
    val g = new TpchDataGenerator(dataDir)
    // g.testDf()
    g.createView()
    val query = Source.fromFile(queryDir).getLines.map(x => x.stripMargin).filter(!_.contains("--")).mkString(" ")
    println(query)
    val start2 = System.nanoTime()
    val df = spark.sql(query)
    df.queryExecution.generateCandidatePlans
    println(s"actual length ${df.queryExecution.candidateSparkPlans.get.length}")
    //println(s"candidate plan generation time: ${NANOSECONDS.toMillis(System.nanoTime() - start2)}")
    df.explain("formatted")
    df.write
      .mode("overwrite")
      .format("noop")
      .save()
    val end = System.nanoTime()
    println(s"total execution time: ${NANOSECONDS.toMillis(end - start)}")
    sc.stop()
  }
}







