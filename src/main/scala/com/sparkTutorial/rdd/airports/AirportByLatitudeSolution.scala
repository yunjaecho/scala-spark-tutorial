package com.sparkTutorial.rdd.airports

import com.sparkTutorial.commons.Utils
import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkConf, SparkContext}

object AirportByLatitudeSolution extends App {
  Logger.getLogger("org").setLevel(Level.ERROR)
  val conf = new SparkConf().setAppName("worldCounts").setMaster("local[2]")
  val sc = new SparkContext(conf)

  val airports = sc.textFile("in/airports.text")
  // 위도가 40이상인 데이터 필터링
  val airportsInUSA = airports.filter(line => line.split(Utils.COMMA_DELIMITER)(6).toFloat > 40)

  val airportsNameAndCityNames = airportsInUSA.map { line =>
    val splits = line.split(Utils.COMMA_DELIMITER)
    splits(1) + ", " + splits(6)
  }
  airportsNameAndCityNames.saveAsTextFile("out/airports_by_latitude.text")
}
