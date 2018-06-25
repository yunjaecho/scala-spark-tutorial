package com.sparkTutorial.advanced.accumulator

import com.sparkTutorial.commons.Utils
import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkConf, SparkContext}

object StackOverFlowSurvey {
  Logger.getLogger("org").setLevel(Level.ERROR)

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("StackOverFlowSurvey").setMaster("local[1]")
    val sparkContext = new SparkContext(conf)

    val total = sparkContext.longAccumulator
    val missingSalaryMidPoint = sparkContext.longAccumulator

    val responseRDD = sparkContext.textFile("in/2016-stack-overflow-survey-responses.csv")

    val responseFromCanada = responseRDD.filter { response =>
      val splits = response.split(Utils.COMMA_DELIMITER, -1)
      total.add(1)

      if (splits(14).isEmpty) {
        missingSalaryMidPoint.add(1)
      }
      splits(2) == "Canada"
    }

    println("Count of responses from Canada : " + responseFromCanada.count())
    println("Totla count of responses: " + total.value)
    println("Count of responses missing salary middle point :" + missingSalaryMidPoint.value)



  }
}
