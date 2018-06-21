package com.sparkTutorial.rdd.sumOfNumbers

import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkConf, SparkContext}

object SumOfNumbersSolution {
  Logger.getLogger("org").setLevel(Level.ERROR)

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("take").setMaster("local[*]")
    val sc = new SparkContext(conf)

    val lines = sc.textFile("in/prime_nums.text")

    val numbers = lines.flatMap(line => line.split("\\s+"))

    val sumIntNumbers = numbers
      .filter(!_.isEmpty)
      .map(_.toInt)
      .reduce(_ + _)

    println(s"sumIntNumbers is $sumIntNumbers")
  }
}
