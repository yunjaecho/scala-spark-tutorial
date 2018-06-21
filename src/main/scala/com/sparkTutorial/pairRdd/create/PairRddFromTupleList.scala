package com.sparkTutorial.pairRdd.create

import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkConf, SparkContext}

object PairRddFromTupleList {
  Logger.getLogger("org").setLevel(Level.ERROR)

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("create").setMaster("local[*]")
    val sc = new SparkContext(conf)

    val tuple = List(("Lily", 23), ("Jack", 29), ("Mary", 29), ("James", 8))
    val pairRDD = sc.parallelize(tuple)

    // coalesce(numPartitions: Int)
    pairRDD.coalesce(1).saveAsTextFile("out/pair_rdd_from_tuple_list")
  }
}
