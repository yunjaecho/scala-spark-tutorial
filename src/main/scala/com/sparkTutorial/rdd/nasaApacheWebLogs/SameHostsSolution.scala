package com.sparkTutorial.rdd.nasaApacheWebLogs

import com.sparkTutorial.rdd.nasaApacheWebLogs.UnionLogsSolution.isNotHeader
import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkConf, SparkContext}

object SameHostsSolution {
  Logger.getLogger("org").setLevel(Level.ERROR)

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("sameHosts").setMaster("local[1]")
    val sc = new SparkContext(conf)

    val julyFirstLog = sc.textFile("in/nasa_19950701.tsv")
    val augustFirstLogs = sc.textFile("in/nasa_19950801.tsv")

    val julyFirstHosts = julyFirstLog.map(line => line.split("\t")(0))
    val augustFirstHosts = augustFirstLogs.map(line => line.split("\t")(0))

    val intersection = julyFirstHosts.intersection(augustFirstHosts)

    val cleanHostIntersection = intersection.filter(host => host != "host")
    cleanHostIntersection.saveAsTextFile("out/sample_nasa_same_hosts.csv")
  }
}
