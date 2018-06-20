package com.sparkTutorial.rdd


import org.apache.log4j.Level
import org.apache.log4j.Logger
import org.apache.spark.{SparkConf, SparkContext}


object WordCount extends App {
  Logger.getLogger("org").setLevel(Level.ERROR)
  val conf = new SparkConf().setAppName("worldCounts").setMaster("local[3]")
  val sc = new SparkContext(conf)

  val lines = sc.textFile("in/word_count.text")
  val words = lines.flatMap(_.split(" "))

  val wordCounts = words.countByValue()
  for ((word, count) <- wordCounts) println(word + " : " + count)
}
