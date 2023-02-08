import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.types.{IntegerType, StringType, StructType}
import org.apache.spark.sql.execution.ExplainMode
import org.apache.spark.sql.functions._

import java.io.File
import scala.concurrent.duration.NANOSECONDS
import scala.io.Source

object QueryTimeRecorder {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      //      .master("local")
      .appName("QueryTimeRecorder")
      .getOrCreate()
      val sc = spark.sparkContext
      sc.setLogLevel("ERROR")
      val dataDir = args(0)
      val queryDir = args(1)
      val resDir = args(2)
      val dataGenerator = new ImdbDataGenerator(dataDir)
      //    a.testDf
      //    val queries = getListOfFiles(queryDir)
      val queries = getQueries(queryDir)
      var res = new Array[(String, String, String, String, String)](0)
      for (q <- queries) {
        spark.catalog.clearCache()
          dataGenerator.createTable
          //      val queryFile = Source.fromFile(q)
          //      val query = queryFile.mkString
          val query = q
          println(q.toString)
          val plan = spark.sql(query).queryExecution.explainString(ExplainMode.fromString("formatted"))
          val plan2 = spark.sql(query).queryExecution.executedPlan
          val start = System.nanoTime()
          spark.sql(query).write
          .mode("overwrite")
          .format("noop")
          .save()
          val end = System.nanoTime()
          val t = NANOSECONDS.toMillis(end - start).toString
          res = res :+ ((q.toString, query, plan2.toString, plan, t))
      }
    spark.createDataFrame(res.toSeq).coalesce(1).write.csv(resDir)
      sc.stop()
  }

  class ImdbDataGenerator(dataDir: String) extends DataGenerator {
    override val spark: SparkSession = SparkSession.builder().getOrCreate()
      // schemas
      val aka_name_schema: StructType = new StructType()
      .add("id", IntegerType, nullable = false)
      .add("person_id", IntegerType, nullable = false)
      .add("name", StringType, nullable = false)
      .add("imdb_index", StringType, nullable = true)
      .add("name_pcode_cf", StringType, nullable = true)
      .add("name_pcode_nf", StringType, nullable = true)
      .add("surname_pcode", StringType, nullable = true)
      .add("md5sum", StringType, nullable = true)

      val aka_title_schema: StructType = new StructType()
      .add("id", IntegerType, nullable = false)
      .add("movie_id", IntegerType, nullable = false)
      .add("title", StringType, nullable = false)
      .add("imdb_index", StringType, nullable = true)
      .add("kind_id", IntegerType, nullable = false)
      .add("production_year", IntegerType, nullable = true)
      .add("phonetic_code", StringType, nullable = true)
      .add("episode_of_id", IntegerType, nullable = true)
      .add("season_nr", IntegerType, nullable = true)
      .add("episode_nr", IntegerType, nullable = true)
      .add("note", StringType, nullable = true)
      .add("md5sum", StringType, nullable = true)

      val cast_info_schema: StructType = new StructType()
      .add("id", IntegerType, nullable = false)
      .add("person_id", IntegerType, nullable = false)
      .add("movie_id", IntegerType, nullable = false)
      .add("person_role_id", IntegerType, nullable = true)
      .add("note", StringType, nullable = true)
      .add("nr_order", IntegerType, nullable = true)
      .add("role_id", IntegerType, nullable = false)

      val char_name_schema: StructType = new StructType()
      .add("id", IntegerType, nullable = false)
      .add("name", StringType, nullable = false)
      .add("imdb_index", StringType, nullable = true)
      .add("imdb_id", IntegerType, nullable = true)
      .add("name_pcode_nf", StringType, nullable = true)
      .add("surname_pcode", StringType, nullable = true)
      .add("md5sum", StringType, nullable = true)

      val comp_cast_type_schema: StructType = new StructType()
      .add("id", IntegerType, nullable = false)
      .add("kind", StringType, nullable = false)

      val company_name_schema: StructType = new StructType()
      .add("id", IntegerType, nullable = false)
      .add("name", StringType, nullable = false)
      .add("country_code", StringType, nullable = true)
      .add("imdb_id", IntegerType, nullable = true)
      .add("name_pcode_nf", StringType, nullable = true)
      .add("name_pcode_sf", StringType, nullable = true)
      .add("md5sum", StringType, nullable = true)

      val company_type_schema: StructType = new StructType()
      .add("id", IntegerType, nullable = false)
      .add("kind", StringType, nullable = false)

      val complete_cast_schema: StructType = new StructType()
      .add("id", IntegerType, nullable = false)
      .add("movie_id", IntegerType, nullable = true)
      .add("subject_id", IntegerType, nullable = false)
      .add("status_id", IntegerType, nullable = false)

      val info_type_schema: StructType = new StructType()
      .add("id", IntegerType, nullable = false)
      .add("info", StringType, nullable = false)

      val keyword_schema: StructType = new StructType()
      .add("id", IntegerType, nullable = false)
      .add("keyword", StringType, nullable = false)
      .add("phonetic_code", StringType, nullable = true)

      val kind_type_schema: StructType = new StructType()
      .add("id", IntegerType, nullable = false)
      .add("kind", StringType, nullable = false)

      val link_type_schema: StructType = new StructType()
      .add("id", IntegerType, nullable = false)
      .add("link", StringType, nullable = false)

      val movie_companies_schema: StructType = new StructType()
      .add("id", IntegerType, nullable = false)
      .add("movie_id", IntegerType, nullable = false)
      .add("company_id", IntegerType, nullable = false)
      .add("company_type_id", IntegerType, nullable = false)
      .add("note", StringType, nullable = true)

      val movie_info_schema: StructType = new StructType()
      .add("id", IntegerType, nullable = false)
      .add("movie_id", IntegerType, nullable = false)
      .add("info_type_id", IntegerType, nullable = false)
      .add("info", StringType, nullable = false)
      .add("note", StringType, nullable = true)

      val movie_info_idx_schema: StructType = new StructType()
      .add("id", IntegerType, nullable = false)
      .add("movie_id", IntegerType, nullable = false)
      .add("info_type_id", IntegerType, nullable = false)
      .add("info", StringType, nullable = false)
      .add("note", StringType, nullable = true)

      val movie_keyword_schema: StructType = new StructType()
      .add("id", IntegerType, nullable = false)
      .add("movie_id", IntegerType, nullable = false)
      .add("keyword_id", IntegerType, nullable = false)

      val movie_link_schema: StructType = new StructType()
      .add("id", IntegerType, nullable = false)
      .add("movie_id", IntegerType, nullable = false)
      .add("linked_movie_id", IntegerType, nullable = false)
      .add("link_type_id", IntegerType, nullable = false)

      val name_schema: StructType = new StructType()
      .add("id", IntegerType, nullable = false)
      .add("name", StringType, nullable = false)
      .add("imdb_index", StringType, nullable = true)
      .add("imdb_id", IntegerType, nullable = true)
      .add("gender", StringType, nullable = true)
      .add("name_pcode_cf", StringType, nullable = true)
      .add("name_pcode_nf", StringType, nullable = true)
      .add("surname_pcode", StringType, nullable = true)
      .add("md5sum", StringType, nullable = true)

      val person_info_schema: StructType = new StructType()
      .add("id", IntegerType, nullable = false)
      .add("person_id", IntegerType, nullable = false)
      .add("info_type_id", IntegerType, nullable = false)
      .add("info", StringType, nullable = false)
      .add("note", StringType, nullable = true)

      val role_type_schema: StructType = new StructType()
      .add("id", IntegerType, nullable = false)
      .add("role", StringType, nullable = false)

      val title_schema: StructType = new StructType()
      .add("id", IntegerType, nullable = false)
      .add("title", StringType, nullable = false)
      .add("imdb_index", StringType, nullable = true)
      .add("kind_id", IntegerType, nullable = false)
      .add("production_year", IntegerType, nullable = true)
      .add("imdb_id", IntegerType, nullable = true)
      .add("phonetic_code", StringType, nullable = true)
      .add("episode_of_id", IntegerType, nullable = true)
      .add("season_nr", IntegerType, nullable = true)
      .add("episode_nr", IntegerType, nullable = true)
      .add("series_years", StringType, nullable = true)
      .add("md5sum", StringType, nullable = true)

      //dataframes
      val akaNameDf: DataFrame = spark.read.schema(aka_name_schema).csv(dataDir + "aka_name.csv")
      val akaTitleDf: DataFrame = spark.read.schema(aka_title_schema).csv(dataDir + "aka_title.csv")
      val castInfoDf: DataFrame = spark.read.schema(cast_info_schema).csv(dataDir + "cast_info.csv")
      //.withColumn("dummy", explode(array((1 until 10).map(lit): _*))).drop("dummy")
      val charNameDf: DataFrame = spark.read.schema(char_name_schema).csv(dataDir + "char_name.csv")
      val compCastTypeDf: DataFrame = spark.read.schema(comp_cast_type_schema).csv(dataDir + "comp_cast_type.csv")
      val companyNameDf: DataFrame = spark.read.schema(company_name_schema).csv(dataDir + "company_name.csv")
      val companyTypeDf: DataFrame = spark.read.schema(company_type_schema).csv(dataDir + "company_type.csv")
      val completeCastDf: DataFrame = spark.read.schema(complete_cast_schema).csv(dataDir + "complete_cast.csv")
      val infoTypeDf: DataFrame = spark.read.schema(info_type_schema).csv(dataDir + "info_type.csv")
      val keywordDf: DataFrame = spark.read.schema(keyword_schema).csv(dataDir + "keyword.csv")
      val kindTypeDf: DataFrame = spark.read.schema(kind_type_schema).csv(dataDir + "kind_type.csv")
      val linkTypeDf: DataFrame = spark.read.schema(link_type_schema).csv(dataDir + "link_type.csv")
      val movieCompaniesDf: DataFrame = spark.read.schema(movie_companies_schema).csv(dataDir + "movie_companies.csv")
      //.withColumn("dummy", explode(array((1 until 20).map(lit): _*)))
      val movieInfoDf: DataFrame = spark.read.schema(movie_info_schema).csv(dataDir + "movie_info.csv")
      //.withColumn("dummy", explode(array((1 until 20).map(lit): _*)))
      val movieInfoIdxDf: DataFrame = spark.read.schema(movie_info_idx_schema).csv(dataDir + "movie_info_idx.csv")
      //.withColumn("dummy", explode(array((1 until 20).map(lit): _*))).drop("dummy")
      val movieKeywordDf: DataFrame = spark.read.schema(movie_keyword_schema).csv(dataDir + "movie_keyword.csv")
      //.withColumn("dummy", explode(array((1 until 20).map(lit): _*))).drop("dummy")
      val movieLinkDf: DataFrame = spark.read.schema(movie_link_schema).csv(dataDir + "movie_link.csv")
      val nameDf: DataFrame = spark.read.schema(name_schema).csv(dataDir + "name.csv")
      val personInfoDf: DataFrame = spark.read.schema(person_info_schema).csv(dataDir + "person_info.csv")
      val roleTypeDf: DataFrame = spark.read.schema(role_type_schema).csv(dataDir + "role_type.csv")
      val titleDf: DataFrame = spark.read.schema(title_schema).csv(dataDir + "title.csv")

      override val dfs: Array[DataFrame] = Array(akaNameDf, akaTitleDf, castInfoDf, charNameDf, compCastTypeDf, companyNameDf,
          companyTypeDf, completeCastDf, infoTypeDf, keywordDf, kindTypeDf, linkTypeDf, movieCompaniesDf, movieInfoDf,
          movieInfoIdxDf, movieKeywordDf, movieLinkDf, nameDf, personInfoDf, roleTypeDf, titleDf)

      override def testDf: Unit = {
        akaNameDf.show(2)
          akaTitleDf.show(2)
          castInfoDf.show(2)
          charNameDf.show(2)
          compCastTypeDf.show(2)
          companyNameDf.show(2)
          companyTypeDf.show(2)
          completeCastDf.show(2)
          infoTypeDf.show(2)
          keywordDf.show(2)
          kindTypeDf.show(2)
          linkTypeDf.show(2)
          movieCompaniesDf.show(2)
          movieInfoDf.show(2)
          movieInfoIdxDf.show(2)
          movieKeywordDf.show(2)
          movieLinkDf.show(2)
          nameDf.show(2)
          personInfoDf.show(2)
          roleTypeDf.show(2)
          titleDf.show(2)
      }

    override def createTable: Unit = {
      akaNameDf.createOrReplaceTempView("aka_name")
        akaTitleDf.createOrReplaceTempView("aka_title")
        castInfoDf.createOrReplaceTempView("cast_info")
        charNameDf.createOrReplaceTempView("char_name")
        compCastTypeDf.createOrReplaceTempView("comp_cast_type")
        companyNameDf.createOrReplaceTempView("company_name")
        companyTypeDf.createOrReplaceTempView("company_type")
        completeCastDf.createOrReplaceTempView("complete_cast")
        infoTypeDf.createOrReplaceTempView("info_type")
        keywordDf.createOrReplaceTempView("keyword")
        kindTypeDf.createOrReplaceTempView("kind_type")
        linkTypeDf.createOrReplaceTempView("link_type")
        movieCompaniesDf.createOrReplaceTempView("movie_companies")
        movieInfoDf.createOrReplaceTempView("movie_info")
        movieInfoIdxDf.createOrReplaceTempView("movie_info_idx")
        movieKeywordDf.createOrReplaceTempView("movie_keyword")
        movieLinkDf.createOrReplaceTempView("movie_link")
        nameDf.createOrReplaceTempView("name")
        personInfoDf.createOrReplaceTempView("person_info")
        roleTypeDf.createOrReplaceTempView("role_type")
        titleDf.createOrReplaceTempView("title")
    }

    def writeTable: Unit = {
      akaNameDf.write.saveAsTable("aka_name")
        akaTitleDf.write.saveAsTable("aka_title")
        castInfoDf.write.saveAsTable("cast_info")
        charNameDf.write.saveAsTable("char_name")
        compCastTypeDf.write.saveAsTable("comp_cast_type")
        companyNameDf.write.saveAsTable("company_name")
        companyTypeDf.write.saveAsTable("company_type")
        completeCastDf.write.saveAsTable("complete_cast")
        infoTypeDf.write.saveAsTable("info_type")
        keywordDf.write.saveAsTable("keyword")
        kindTypeDf.write.saveAsTable("kind_type")
        linkTypeDf.write.saveAsTable("link_type")
        movieCompaniesDf.write.saveAsTable("movie_companies")
        movieInfoDf.write.saveAsTable("movie_info")
        movieInfoIdxDf.write.saveAsTable("movie_info_idx")
        movieKeywordDf.write.saveAsTable("movie_keyword")
        movieLinkDf.write.saveAsTable("movie_link")
        nameDf.write.saveAsTable("name")
        personInfoDf.write.saveAsTable("person_info")
        roleTypeDf.write.saveAsTable("role_type")
        titleDf.write.saveAsTable("title")
    }

    override val dfNameArr: Array[(String, String)] = Array(("akaNameDf", "aka_name"),
        ("akaTitleDf", "aka_title"),
        ("castInfoDf", "cast_info"),
        ("charNameDf", "char_name"),
        ("compCastTypeDf", "comp_cast_type"),
        ("companyNameDf", "company_name"),
        ("companyTypeDf", "company_type"),
        ("completeCastDf", "complete_cast"),
        ("infoTypeDf", "info_type"),
        ("keywordDf", "keyword"),
        ("kindTypeDf", "kind_type"),
        ("linkTypeDf", "link_type"),
        ("movieCompaniesDf", "movie_companies"),
        ("movieInfoDf", "movie_info"),
        ("movieInfoIdxDf", "movie_info_idx"),
        ("movieKeywordDf", "movie_keyword"),
        ("movieLinkDf", "movie_link"),
        ("nameDf", "name"),
        ("personInfoDf", "person_info"),
        ("roleTypeDf", "role_type"),
        ("titleDf", "title"))

          val dfNameMap: Map[String, String] = dfNameArr.toMap

  }


  def getListOfFiles(dir: String): List[File] = {
    val d = new File(dir)
      if (d.exists && d.isDirectory) {
        d.listFiles.filter(_.isFile).toList
      } else {
        List[File]()
      }
  }

  def getQueries(dir: String): Array[String] = {
    val d = new File(dir)
      val files = d.listFiles.filter(_.isFile).toList.filter(x => x.getName.contains(".sql"))
      println(files)
      var queries = new Array[String](0)
      for (file <- files) {
        for (line <- Source.fromFile(file).getLines) {
          queries = queries :+ line
        }
      }
    println(s"== found ${queries.length} queries")
      queries
  }
}


