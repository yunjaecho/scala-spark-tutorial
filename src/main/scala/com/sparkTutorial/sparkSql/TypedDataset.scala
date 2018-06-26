package com.sparkTutorial.sparkSql

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession

object TypedDataset {
  val AGE_MIDPOINT = "age_midpoint"
  val SALARY_MIDPOINT = "salary_midpoint"
  val SALARY_MIDPOINT_BUCKET = "salaryMidpointBucket"

  Logger.getLogger("org").setLevel(Level.ERROR)

  def main(args: Array[String]): Unit = {
    val session = SparkSession.builder().appName("TypedDataset").master("local[*]").getOrCreate()
    val dataFrameReader = session.read

    val responses = dataFrameReader
      .option("header", "true")
      .option("inferSchema", value = true)
      .csv("in/2016-stack-overflow-survey-responses.csv")

    val responseWithSelectedColumns = responses.select("country", "age_midpoint", "occupation", "salary_midpoint")

    import session.implicits._
    val typedDataset = responseWithSelectedColumns.as[Resonse]

    println("=== Print out schema ===")
    typedDataset.printSchema()

    println("=== Print 20 records of responses table ===")
    typedDataset.show(20)

    println("=== Print the responses from Afghanistan ===")
    typedDataset.filter(_.country == "Afghanistan").show()

    println("=== Print the count fo occupations ===")
    typedDataset.groupBy(typedDataset.col("occupation")).count().show()

    println("=== Print responses with average mid age les than 20 ===")
    typedDataset.filter(response => response.age_midpoint.getOrElse(0.0) < 20.0).show()

    println("=== Print the result by salary middle point in desending order ===")
    typedDataset.orderBy(typedDataset.col(SALARY_MIDPOINT).desc).show()

    println("=== Group by country and aggregate by average salary middle point ===")
    typedDataset.filter(_.salary_midpoint.isDefined).groupBy("country").avg(SALARY_MIDPOINT).show()

    println("=== Group by salary bucket ===")
    typedDataset
      .map(response => response.salary_midpoint.map(point => Math.round(point/ 20000) * 20000).orElse(None))
      .withColumnRenamed("value", SALARY_MIDPOINT_BUCKET)
      .groupBy(SALARY_MIDPOINT_BUCKET)
      .count()
      .orderBy(SALARY_MIDPOINT_BUCKET)
      .show()

  }
}
