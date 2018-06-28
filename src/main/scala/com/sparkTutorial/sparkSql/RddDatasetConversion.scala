package com.sparkTutorial.sparkSql

import com.sparkTutorial.commons.Utils
import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SparkSession

object RddDatasetConversion {
  Logger.getLogger("org").setLevel(Level.ERROR)

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("RddDatasetConversion").setMaster("local[1]")
    val sc = new SparkContext(conf)
    val session = SparkSession.builder().appName("RddDatasetConversion").master("local[*]").getOrCreate()

    val lines = sc.textFile("in/2016-stack-overflow-survey-responses.csv")

    val resonseRDD = lines
      .filter(line => !line.split(Utils.COMMA_DELIMITER, -1)(2).equals("country"))
      .map(line => {
        val splits = line.split(Utils.COMMA_DELIMITER, -1)
        com.sparkTutorial.sparkSql.Resonse(splits(2), toDouble(splits(6)), splits(9), toDouble(splits(14)))
      })

    import session.implicits._
    val resonseDataset = resonseRDD.toDS()

    println("=== Print out schema ===")
    resonseDataset.printSchema()

    println("=== Print 20 records of response table ===")
    resonseDataset.show(20)

    for (respnse <- resonseDataset.rdd.collect()) println(respnse)
  }

  def toDouble(split: String): Option[Double] = {
    if (split.isEmpty) None else Some(split.toDouble)
  }

}
