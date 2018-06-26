package com.sparkTutorial.sparkSql

import com.sparkTutorial.commons.Utils
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.{SparkConf, SparkContext}

object StackOverFlowSurvey {
  Logger.getLogger("org").setLevel(Level.ERROR)

  val AGE_MIDPOINT = "age_midpoint"
  val SALARY_MIDPOINT = "salary_midpoint"
  val SALARY_MIDPOINT_BUCKET = "salary_midpoint_bucket"

  def main(args: Array[String]): Unit = {
    val session = SparkSession.builder().appName("StackOverFlowSurvey").master("local[1]").getOrCreate()

    val dataFrameReader = session.read

    val responses = dataFrameReader
      .option("header", "true")
      .option("inferSchema", value = true)
      .csv("in/2016-stack-overflow-survey-responses.csv")

    println("======== Print out Schema ========")
    responses.printSchema()

    val responseWithSelectedColumns = responses.select("country", "occupation", AGE_MIDPOINT, SALARY_MIDPOINT)
    println("=== Print the selected columns of the table ===")
    responseWithSelectedColumns.show()

    println("=== Print records where the responses is from Afghanistan ===")
    responseWithSelectedColumns.filter(responseWithSelectedColumns.col("country").===("Afghanistan")).show()

    println("=== Print the count of occupations ===")
    val groupedDataset = responseWithSelectedColumns.groupBy("occupation")
    groupedDataset.count().show()

    println("=== Print records with average mid age less than 20 ===")
    responseWithSelectedColumns.filter(responseWithSelectedColumns.col(AGE_MIDPOINT) < 20).show()

    println("=== Print the result by salary middle point in descending order ===")
    responseWithSelectedColumns.orderBy(responseWithSelectedColumns.col(SALARY_MIDPOINT).desc).show()

    println("=== Group by country and aggregate by average salary middle point and max age middle point ==")
    val datasetGroupByCountry = responseWithSelectedColumns.groupBy("country")
    datasetGroupByCountry.avg(SALARY_MIDPOINT).show()

    val responseWithSalaryBucket = responses.withColumn(SALARY_MIDPOINT_BUCKET,
      responses.col(SALARY_MIDPOINT).divide(20000).cast("integer").multiply(20000))

    println("=== With salary bucket column ===")
    responseWithSalaryBucket.select(SALARY_MIDPOINT, SALARY_MIDPOINT_BUCKET).show()

    println("=== Group by salary bucket ===")
    responseWithSalaryBucket.groupBy(SALARY_MIDPOINT_BUCKET).count().orderBy(SALARY_MIDPOINT_BUCKET).show()

    session.stop()
  }
}
