package com.sparkTutorial.sparkSql

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{SparkSession, functions}

case class AccInfo(no: Int,userId: String, accDt: String, accIp: String, act: String)

object PosmallAcct {
  Logger.getLogger("org").setLevel(Level.ERROR)

  def main(args: Array[String]): Unit = {
    val session = SparkSession.builder().appName("PosmallAcct").master("local[*]").getOrCreate()

    val accOld = session.read
      .option("header", "true")
      .option("inferSchema", value = true)
      .csv("in/posmall_acc_odl_1.csv")

    val accNew = session.read
      .option("header", "true")
      .option("inferSchema", value = true)
      .csv("in/posmall_acc_new_1.csv")

    accNew.show()


    import session.implicits._
    val typedAccNew = accNew.as[AccInfo]
    val typedAccOld = accOld.as[AccInfo]

    val changAccOld = typedAccOld
      .groupBy(typedAccOld.col("userId"))
      .agg(functions.max(typedAccOld.col("accIp"))).alias("accIp")
    changAccOld.printSchema()

    //  makerSpace.join(postCode, makerSpace.col("Postcode").startsWith(postCode.col("Postcode")), "left_outer")
    //val changeAccNew = typedAccNew.joinWith(changAccOld, changeAccNewcol("key") === changAccOld.col("key"), "left_outer")
    typedAccNew.joinWith(changAccOld, typedAccNew.col("userId") === changAccOld.col("userId"), "left_outer").show()
    //changeAccNew.show()

  }
}
