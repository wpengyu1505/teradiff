package wpy.graphlinker.core

import io.rainbow6.teradiff.core.model.KeyValue
import org.apache.spark.rdd.RDD
import org.apache.spark.Accumulator
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}

import scala.collection.mutable.ListBuffer
import scala.collection.mutable.HashSet

class TeraCompare (spark:SparkSession, leftDf:DataFrame, leftExpr:(String, String), rightDf:DataFrame, rightExpr:(String, String)) {

  import spark.implicits._
  var leftData:Dataset[KeyValue] = leftDf.selectExpr(leftExpr._1, leftExpr._2).as[KeyValue]
  var rightData:Dataset[KeyValue] = rightDf.selectExpr(rightExpr._1, rightExpr._2).as[KeyValue]

  def compare() = {

    val joined = leftData.joinWith(rightData, leftData.col("key") === rightData.col("key"), "outer")
    val out = joined.map(v => {
      val left = v._1
      val right = v._2

      println("Left: %s, right: %s".format(left.value, right.value))
      left.value + "---" + right.value
    })

    out
  }

}