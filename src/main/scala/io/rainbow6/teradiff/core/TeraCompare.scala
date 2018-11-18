package wpy.graphlinker.core

import io.rainbow6.teradiff.core.model.{ComparisonResult, KeyValue}
import io.rainbow6.teradiff.expression.Constants
import org.apache.commons.lang.StringUtils
import org.apache.spark.rdd.RDD
import org.apache.spark.Accumulator
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}
import org.apache.spark.util.LongAccumulator

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
        val rightCols = StringUtils.splitPreserveAllTokens(right.value, Constants.delimiter)

        var isDiff = false
        val sb = new StringBuilder()

        for (i <- 0 to leftCols.length - 1) {
          if (leftCols(i) != rightCols(i)) {
            isDiff = true
            sb.append("[%s: (%s, %s)],".format(i, leftCols(i), rightCols(i)))
          }
        }

        if (isDiff) {
          diffAccumulator.add(1)
          sb.setLength(sb.length - 1)
          list.append(ComparisonResult(left.key, sb.toString(), "diff"))
        }
      }

      list.toList
    })

    (out, leftAccumulator, rightAccumulator, diffAccumulator)
  }

  def analyzeResult(result:(Dataset[ComparisonResult], LongAccumulator, LongAccumulator, LongAccumulator)) = {

    val dataset = result._1
    dataset.persist()
    //dataset.count()

    val left = dataset.filter(v => v.recType == "left").take(100)
    val right = dataset.filter(v => v.recType == "right").take(100)
    val diff = dataset.filter(v => v.recType == "diff").take(100)

    val leftOnlyCount = result._2.value
    val rightOnlyCount = result._3.value
    val diffCount = result._4.value

    println("============= Stats =============")
    println("LHS only:   %s".format(leftOnlyCount))
    println("RHS only:   %s".format(rightOnlyCount))
    println("Difference: %s".format(diffCount))
    println("============= Left ==============")
    left.foreach(println)

    println("============= right =============")
    right.foreach(println)

    println("============= diff ==============")
    diff.foreach(println)
  }

}