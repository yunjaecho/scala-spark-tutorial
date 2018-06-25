package com.sparkTutorial.pairRdd.join

import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkConf, SparkContext}

object JoinOperation {
  Logger.getLogger("org").setLevel(Level.ERROR)

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("worldCounts").setMaster("local[3]")
    val sc = new SparkContext(conf)

    val ages = sc.parallelize(List(("Tom", 29), ("John", 22)))
    val addressed = sc.parallelize(List(("James", "USA"), ("John", "UK")))

    val join = ages.join(addressed)
    join.saveAsTextFile("out/age_adress_join.txt")

    val leftOuterJoin = ages.leftOuterJoin(addressed)
    leftOuterJoin.saveAsTextFile("out/age_address_left_out_join.txt")

    val rightOuterJoin = ages.rightOuterJoin(addressed)
    rightOuterJoin.saveAsTextFile("out/age_address_right_out_join.txt")

    val fullOuterJoin = ages.fullOuterJoin(addressed)
    fullOuterJoin.saveAsTextFile("out/age_address_full_out_join")
  }
}
