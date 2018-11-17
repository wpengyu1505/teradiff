package io.rainbow6.teradiff.runner

import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}
import org.apache.spark.SparkConf

object TeradiffRunner {

  def main(args:Array[String]): Unit = {

    val conf = new SparkConf().setAppName("TeraDiff").setMaster("local")
    val spark = SparkSession.builder.config(conf).getOrCreate()

    //val df1 =
  }
}
