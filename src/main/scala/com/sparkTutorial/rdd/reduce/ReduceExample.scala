package com.sparkTutorial.rdd.reduce

import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkConf, SparkContext}

object ReduceExample {
  Logger.getLogger("org").setLevel(Level.ERROR)

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("take").setMaster("local[*]")
    val sc = new SparkContext(conf)

    val inputIntegers = List(1,2,3,4,5)
    val integerRdd = sc.parallelize(inputIntegers)

    val product = integerRdd.reduce( _ * _)
    println(s"product id $product")
  }
}
