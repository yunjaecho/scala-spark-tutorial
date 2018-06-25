package com.sparkTutorial.pairRdd.sort

import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkConf, SparkContext}

object SortedWordCountSolution {
  Logger.getLogger("org").setLevel(Level.ERROR)

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("wordCounts").setMaster("local[3]")
    val sc = new SparkContext(conf)

    val lines = sc.textFile("in/word_count.text")
    val wordRdd = lines.flatMap(_.split(" "))

    val wordPairRdd = wordRdd.map((_, 1))
    val wordToCountPairs = wordPairRdd.reduceByKey(_ + _)

    val countToWordPairs = wordToCountPairs.map(wordToCount => (wordToCount._2, wordToCount._1))

    val sortedCountToWordPairs = countToWordPairs.sortByKey(ascending = false, numPartitions= 1)

    val result = sortedCountToWordPairs.map(countToWord => (countToWord._2, countToWord._1))
    for ((word, count) <- result) {
      println(word + " : " + count)
    }
  }
}
