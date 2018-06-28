package com.sparkTutorial.sparkSql

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{SparkSession, functions}

case class AccInfo(no: Int,userId: String, accDt: String, accIp: String, act: String)

/**
  * 1. 과거에 로그 이력의 IP 정보를 ID별로 그룹핑 처리
  * 2. 신규 로그 이력의 ID와 1번 처리된 데이터셋을 조인
  * 3. 2번 처리의 결과 값을 csv 파일로 저장처리
  */
object PosmallAcct {
  Logger.getLogger("org").setLevel(Level.ERROR)

  def main(args: Array[String]): Unit = {
    val session = SparkSession.builder().appName("PosmallAcct").master("local[*]").getOrCreate()

    import session.implicits._

    // 이전 로그 이력 파일 로드
    val accInfoOld = session.read
      .option("header", "true")
      .option("inferSchema", value = true)
      .csv("in/posmall_acc_odl_1.csv")
      .as[AccInfo]

    // 현재 로그 이력 파일 로드
    val accInfoNew = session.read
      .option("header", "true")
      .option("inferSchema", value = true)
      .csv("in/posmall_acc_new_1.csv")
      .as[AccInfo]


    // 이전 로그 이력 파일 userId로 그룹 하면서 accIp max 값만 추출
    val accInfoOld2 = accInfoOld
      .groupBy(accInfoOld.col("userId"))
      .agg(functions.max(accInfoOld.col("accIp")).alias("accIp"))

    accInfoOld2.printSchema()

    // 데이터 조인 처리
    val resultDataset = accInfoNew
      .as("data1")
      .join(accInfoOld2.as("data2"), Seq("userId"), "left_outer")
      //.select(accInfoNew.col("no"), accInfoNew.col("userId"), accInfoNew.col("accDt"), accInfoOld2.col("accIp"), accInfoNew.col("act"))
      //.select($"data1.*")
      .select("data1.no", "data1.userId", "data1.accDt", "data2.accIp", "data1.act")

    // 데이터 csv 저장 처리
    resultDataset
      .write
      .format("csv")
      .save("out/posmall_accInfo")


  }
}
