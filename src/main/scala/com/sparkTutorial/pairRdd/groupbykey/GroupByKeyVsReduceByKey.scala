package com.sparkTutorial.pairRdd.groupbykey

import com.sparkTutorial.commons.Utils
import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkConf, SparkContext}

object GroupByKeyVsReduceByKey {
  Logger.getLogger("org").setLevel(Level.ERROR)

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("GroupByKeyVsReduceByKey").setMaster("local[*]")
    val sc = new SparkContext(conf)

    val words = List("one", "two", "two", "three", "three", "three")
    val wordsPairRdd = sc.parallelize(words).map(word => (words, 1))

    // GroupByKey 보다 더좋은 성능(reduceByKey)
    val wordCountsWithReduceByKey = wordsPairRdd.reduceByKey((x, y) => x + y).collect()
    println("wordCountsWithReduceByKey : " + wordCountsWithReduceByKey.toList)

    val wordCountsWithGroupByKey = wordsPairRdd.groupByKey().mapValues(intIterable => intIterable.size).collect
    println("wordCountsWithGroupByKey : " + wordCountsWithGroupByKey.toList)
  }
}
