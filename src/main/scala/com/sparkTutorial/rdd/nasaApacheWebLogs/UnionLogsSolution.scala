package com.sparkTutorial.rdd.nasaApacheWebLogs

import com.sparkTutorial.commons.Utils
import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkConf, SparkContext}

object UnionLogsSolution  {

  Logger.getLogger("org").setLevel(Level.ERROR)

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("worldCounts").setMaster("local[*]")
    val sc = new SparkContext(conf)

    val julyFirstLog = sc.textFile("in/nasa_19950701.tsv")
    val augustFirstLogs = sc.textFile("in/nasa_19950801.tsv")

    val aggregatedLogLines = julyFirstLog.union(augustFirstLogs)

    val cleanLogLines = aggregatedLogLines.filter(line => isNotHeader(line))

    val sample = cleanLogLines.sample(withReplacement = true, fraction = 0.1)

    sample.saveAsTextFile("out/sample_nasa_logs.csv")
  }


  def isNotHeader(line: String): Boolean = !(line.startsWith("host") && line.contains("bytes"))
}
