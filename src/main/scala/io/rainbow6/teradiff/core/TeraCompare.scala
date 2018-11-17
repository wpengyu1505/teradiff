package wpy.graphlinker.core

import io.rainbow6.teradiff.core.model.{ComparisonResult, KeyValue}
import io.rainbow6.teradiff.expression.Constants
import org.apache.commons.lang.StringUtils
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

    val leftAccumulator = spark.sparkContext.longAccumulator("LHS")
    val rightAccumulator = spark.sparkContext.longAccumulator("RHS")
    val diffAccumulator = spark.sparkContext.longAccumulator("diff")

    val out = joined.flatMap(v => {

      val list = new ListBuffer[ComparisonResult]

      val left = v._1
      val right = v._2

      var leftString = "[none]"
      var rightString = "[none]"
      if (left != null) {
        leftString = left.value
      }
      if (right != null) {
        rightString = right.value
      }

      if (left == null) {

        rightAccumulator.add(1)
        list.append(ComparisonResult(right.key, right.value, "right"))

      } else if (right == null) {

        leftAccumulator.add(1)
        list.append(ComparisonResult(left.key, left.value, "left"))

      } else {

        val leftCols = StringUtils.splitPreserveAllTokens(left.value, Constants.delimiter)
        val rightCols = StringUtils.splitPreserveAllTokens(left.value, Constants.delimiter)

        val sb = new StringBuilder()

        for (i <- 0 to leftCols.length - 1) {
          if (leftCols(i) != rightCols(i)) {
            sb.append("[%s: (%s, %s)]".format(i, leftCols(i), rightCols(i)))
            if (i < leftCols.length - 1) {
              sb.append(", ")
            }
          }
        }

        list.append(ComparisonResult(left.key, sb.toString(), "diff"))
      }

      list.toList
    })

    out
  }

}