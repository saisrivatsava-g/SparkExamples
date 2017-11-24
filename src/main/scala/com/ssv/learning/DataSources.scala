package com.ssv.learning
import org.apache.log4j._
import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.StructField
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.Row

object DataSources extends App {
  Logger.getLogger("org").setLevel(Level.ERROR)
  val sc = new SparkContext("local[*]", "sc")
  val spark = SparkSession.builder().appName("DataSources").getOrCreate()
  val resPath = "../spark_programs/resources/"

  import spark.implicits._

  val inputData = spark.sparkContext.textFile(resPath + "PoliceData.csv")
  val header = inputData.first()
  val dataWithoutHeader = inputData.filter { !_.contains(header) }

  //cleaning the input data
  val cdata = dataWithoutHeader.map(cleanData)

  //creating the schema
  val sFields = header.split(",").map { field => StructField(field, StringType, true) }
  val schema = StructType(sFields)

  import org.apache.spark.sql.Row._

  //creating RDD[Row]
  val rows = cdata.map(_.split(",")).map { fields => Row.fromSeq(fields) }

  //creating the DF
  val dataDF = spark.createDataFrame(rows, schema)

  // dataDF.show(20, false)

  //dataDF.printSchema()

  //saving data into parquet form
  dataDF.write.format("parquet").save(resPath + "PoliceData.parquet")

  //saving data into json
  //dataDF.write.format("json").save(resPath + "policeData.json")
  //println("success...")

  //loading parquet data 

    val parquetDataDF = spark.read.format("parquet").parquet(resPath + "PoliceData.parquet")
    println(parquetDataDF.count())
    parquetDataDF.show(20)

  //loading json data 

  //val jsonDataDF = spark.read.format("json").parquet(resPath + "policeData.json")
  //  println(jsonDataDF.count())
  //jsonDataDF.show(20)

  def cleanData(inputLine: String) = {
    var flag = 1
    var line = inputLine.toBuffer
    for (i <- 0 to line.size - 1) {
      if (line(i) == '\"') {
        // line.remove(i)
        flag += 1
      } else if (flag % 2 == 0 && line(i) == ',') {
        line(i) = '&'
        flag = 0
      }
    }
    line.mkString.replaceAll("\"", "")
  }
}