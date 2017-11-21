package com.ssv.learning
import org.apache.log4j.Level
import org.apache.log4j.Logger
import org.apache.spark.SparkContext
object WordCount extends App {

  Logger.getLogger("org").setLevel(Level.ERROR)
  val sc = new SparkContext("local[*]", "SparkHelloWorld")
  val resPath = "../spark_programs/resources/"
  val data = sc.textFile(resPath + "LICENSE")

  val wordsRDD = data.flatMap(line => line.split(" ")).map(words => (words, 1)).reduceByKey(_ + _)
  val words = wordsRDD.collect()
  val numOfWords = wordsRDD.count()
  println("total number of words in the file : " + numOfWords)
  println("words are ")
  words.foreach(println)

}