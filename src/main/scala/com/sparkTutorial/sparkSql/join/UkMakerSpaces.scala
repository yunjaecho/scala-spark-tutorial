package com.sparkTutorial.sparkSql.join

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{SparkSession, functions}

object UkMakerSpaces {
  Logger.getLogger("org").setLevel(Level.ERROR)

  def main(args: Array[String]): Unit = {
    val session = SparkSession.builder().appName("HousePriceSolution").master("local[1]").getOrCreate()

    val makerSpace = session.read
      .option("header", "true")
      .option("inferSchema", value = true)
      .csv("in/uk-makerspaces-identifiable-data.csv")

    val postCode = session.read
      .option("header", "true")
      .option("inferSchema", value = true)
      .csv("in/uk-postcode.csv")
      // "AB1 "
      .withColumn("Postcode", functions.concat_ws("", functions.col("Postcode"), functions.lit(" ")))

    println("=== Print 20 records of makerspace table ===")
    makerSpace.select("Name of makerspace", "Postcode").show()

    println("=== Print 20 records of postcode table ===")
    postCode.show()

    val joined = makerSpace.join(postCode, makerSpace.col("Postcode").startsWith(postCode.col("Postcode")), "left_outer")

    println("=== Group by Region ===")
    joined.groupBy("Region").count().show(200)


  }
}
