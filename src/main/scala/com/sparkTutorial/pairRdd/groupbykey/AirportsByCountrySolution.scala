package com.sparkTutorial.pairRdd.groupbykey

import com.sparkTutorial.commons.Utils
import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkConf, SparkContext}

object AirportsByCountrySolution {
  Logger.getLogger("org").setLevel(Level.ERROR)

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("groupbykey").setMaster("local[*]")
    val sc = new SparkContext(conf)

    val lines = sc.textFile("in/airports.text")

    val countryAndAirportNameAndPair = lines.map { airport =>
      val splitAirpost = airport.split(Utils.COMMA_DELIMITER)
      (splitAirpost(3), splitAirpost(1))
    }

    val airportsByCountry = countryAndAirportNameAndPair.groupByKey()

    for ((country, airportName) <- airportsByCountry.collectAsMap()) println(country + " : " + airportName.toList)
  }
}
