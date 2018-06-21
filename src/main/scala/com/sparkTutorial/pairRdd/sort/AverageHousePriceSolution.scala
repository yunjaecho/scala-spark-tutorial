package com.sparkTutorial.pairRdd.sort

import com.sparkTutorial.pairRdd.aggregation.reducebykey.housePrice.AvgCount
import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkConf, SparkContext}

object AverageHousePriceSolution {
  Logger.getLogger("org").setLevel(Level.ERROR)

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("sort").setMaster("local[*]")
    val sc = new SparkContext(conf)

    val lines = sc.textFile("in/RealEstate.csv")
    val cleanedLines = lines.filter(line => !line.contains("Bedrooms"))

    // MLS,Location,Price,Bedrooms,Bathrooms,Size,Price SQ Ft,Status
    // 3) Bedrooms
    // 2) Price
    val hosePricePairRdd = cleanedLines.map { line =>
      val splitLine = line.split(",")
      (splitLine(3), AvgCount(1, splitLine(2).toDouble))
    }

    val hosePriceTotal = hosePricePairRdd.reduceByKey((x, y) => AvgCount(x.count + y.count, x.total + y.total))

    val housePriceAvg = hosePriceTotal.mapValues(avgCount => avgCount.total / avgCount.count)

    val sortedHousePriceAvg = housePriceAvg.sortByKey(ascending = false)

    for ((bedrooms , avgPrice) <- sortedHousePriceAvg.collect()) println(s"$bedrooms : $avgPrice")

  }
}
