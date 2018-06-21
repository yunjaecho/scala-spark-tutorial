package com.sparkTutorial.rdd.persist

import org.apache.log4j.{Level, Logger}
import org.apache.spark.storage.StorageLevel
import org.apache.spark.{SparkConf, SparkContext}

object PersistExample {
  Logger.getLogger("org").setLevel(Level.ERROR)

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("persist").setMaster("local[*]")
    val sc = new SparkContext(conf)

    val inputIntegers = List(1,2,3,4,5)
    val integerRdd = sc.parallelize(inputIntegers)

    integerRdd.persist(StorageLevel.MEMORY_ONLY)

    integerRdd.reduce( _ * _)
    integerRdd.count()
  }
}
