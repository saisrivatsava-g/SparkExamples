package com.ssv.learning

import org.apache.log4j._
import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession

object SparkSessionExample extends App {
  Logger.getLogger("org").setLevel(Level.ERROR)

  val sc = new SparkContext("local[*]", "SC")
  val spark = SparkSession.builder.appName("Police Data").getOrCreate()
  val resPath = "../spark_programs/resources/"

  //loading CSV file using SCHEMA
  val inputFile = sc.textFile(resPath + "PoliceData.csv").cache()
  val header = inputFile.first()
  import org.apache.spark.sql.types._
  val fields = header.split(",").map(field => StructField(field, StringType, false))
  val schema = StructType(fields)
  val data = inputFile.filter(!_.contains(header))
  import org.apache.spark.sql.Row
  val rows = data.map(_.split(",")).map(fields => Row.fromSeq(fields))
  val dataDf = spark.createDataFrame(rows, schema)
  dataDf.show(20)

  //creating a table
  //  val dataTable = dataDf.createOrReplaceTempView("policeData")

  //querrying table
  //  val mondayData = spark.sql("select IncidntNum, Category, DayOfWeek from policeData WHERE DayOfWeek == \"Saturday\" ")

  //mondayData.show(10)
  //  println(mondayData.count())

}