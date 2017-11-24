package com.ssv.learning
import org.apache.log4j._
import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession

object Datasets extends App {
  Logger.getLogger("org").setLevel(Level.ERROR)

  val sc = new SparkContext("local[*]", "Sc")
  val spark = SparkSession.builder().appName("DatasetsExamples").getOrCreate()
  val resPath = "../spark_programs/resources/"

  //creating a dataset from Case classes
  import spark.implicits._

  case class Friends(firstName: String, lastName: String)

  val friends = Seq(Friends("Ross", "Geller"), Friends("Rachel", "Greene"), Friends("Monika", "Geller"), Friends("Chandler", "Bing"), Friends("Phoebe", "Buffey"), Friends("Joey", "Tribbiani")).toDS()

  //friends.show()
  //friends.printSchema()
  
  //creating DataSets from Collections
  val evenDS = Seq(2, 4, 6, 8, 10, 12).toDS
  //evenDS.show()
  val oddDS = evenDS.map(_ + 1)
  //oddDS.show()

  //creating DataSets from RDD

  evenDS.createOrReplaceTempView("et")
  spark.sql("select * from et").show()
}