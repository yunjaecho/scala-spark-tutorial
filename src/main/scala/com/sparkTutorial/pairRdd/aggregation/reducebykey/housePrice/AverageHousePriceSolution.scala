package com.sparkTutorial.pairRdd.aggregation.reducebykey.housePrice

import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkConf, SparkContext}

object AverageHousePriceSolution {
  Logger.getLogger("org").setLevel(Level.ERROR)

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("create").setMaster("local[*]")
    val sc = new SparkContext(conf)

    val lines = sc.textFile("in/RealEstate.csv")
    val cleanedLines = lines.filter(line => !line.contains("Bedrooms"))

    // MLS,Location,Price,Bedrooms,Bathrooms,Size,Price SQ Ft,Status
    // 3) Bedrooms
    // 2) Price
    val hosePricePairRdd = cleanedLines.map { line =>
      val splitLine = line.split(",")
      (splitLine(3), (1, splitLine(2).toDouble))
    }

    val hosePriceTotal = hosePricePairRdd.reduceByKey((x, y) => (x._1 + y._1, x._2 + y._2))

    println("housePriceTotal...")
    for ((bedroom, total) <- hosePriceTotal.collect()) println(bedroom + ": " + total)

    val housePriceAvg = hosePriceTotal.mapValues(avgCount => avgCount._2 / avgCount._1)
    println("housePriceAvg")
    for ((bedroom, avg) <- housePriceAvg.collect()) println(bedroom + ": " + avg)

  }
}
