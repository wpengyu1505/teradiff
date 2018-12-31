package io.rainbow6.teradiff.core

import java.io.PrintWriter

import io.rainbow6.teradiff.core.model.{ComparisonResult, KeyValue}
import io.rainbow6.teradiff.expression.{Constants, ExpressionBuilder}
import org.apache.commons.lang.StringUtils
import org.apache.spark.rdd.RDD
import org.apache.spark.Accumulator
import org.apache.spark.sql.types.{DecimalType, StructType}
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}
import org.apache.spark.util.LongAccumulator

import scala.collection.mutable.ListBuffer
import scala.collection.mutable.HashSet

class TeraCompare (spark:SparkSession, expression:ExpressionBuilder, leftDf:DataFrame, rightDf:DataFrame) extends Serializable {

  import spark.implicits._
  var leftData:Dataset[KeyValue] = leftDf.selectExpr(expression.getLeftKeyExpr(), expression.getLeftValueExpr()).as[KeyValue]
  var rightData:Dataset[KeyValue] = rightDf.selectExpr(expression.getRightKeyExpr(), expression.getRightValueExpr()).as[KeyValue]

  val exp = spark.sparkContext.broadcast(expression)

  def compare() = {

    val joined = leftData.joinWith(rightData, leftData.col("key") === rightData.col("key"), "outer")

    val leftAccumulator = spark.sparkContext.longAccumulator("LHS")
    val rightAccumulator = spark.sparkContext.longAccumulator("RHS")
    val diffAccumulator = spark.sparkContext.longAccumulator("diff")

    val out = joined.flatMap(v => {

      val list = new ListBuffer[ComparisonResult]

      // Expression is broadcasted object
      val expression = exp.value

      // For result, use left mapping assuming both sides are same
      val columnMap = expression.getColumnMap()

      val left = v._1
      val right = v._2

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
            sb.append("[%s: (%s, %s)],".format(columnMap.get(i).get, leftCols(i), rightCols(i)))
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

  def analyzeResult(result:(Dataset[ComparisonResult], LongAccumulator, LongAccumulator, LongAccumulator), writer:PrintWriter) = {

    val dataset = result._1
    dataset.persist()
    //dataset.count()

    val left = dataset.filter(v => v.recType == "left").take(100)
    val right = dataset.filter(v => v.recType == "right").take(100)
    val diff = dataset.filter(v => v.recType == "diff").take(100)

    val leftOnlyCount = result._2.value
    val rightOnlyCount = result._3.value
    val diffCount = result._4.value

    val leftCount = leftDf.count
    val rightCount = rightDf.count

    val sum = leftOnlyCount + rightOnlyCount
    val leftPercentage = (1 - sum.toFloat / leftCount.toFloat) * 100
    val rightPercentage = (1 - sum.toFloat / rightCount.toFloat) * 100



    writeLine("============= Total count ==============", writer)
    writeLine("LHS count:  %s".format(leftCount), writer)
    writeLine("RHS count:  %s".format(rightCount), writer)

    writeLine("============= Percentage ==============", writer)
    writeLine("LHS Match: %s".format(leftPercentage + "%"), writer)
    writeLine("RHS Match: %s".format(rightPercentage + "%"), writer)
    writeLine("============= Stats =============", writer)
    writeLine("LHS only:   %s".format(leftOnlyCount), writer)
    writeLine("RHS only:   %s".format(rightOnlyCount), writer)
    writeLine("Difference: %s".format(diffCount), writer)
    writeLine("============= Left side only ==============", writer)

    left.foreach(v => {
      writeLine(v.toString(), writer)
    })

    writeLine("============= Right side only =============", writer)
    right.foreach(v => {
      writeLine(v.toString(), writer)
    })

    writeLine("============= Column differences ==============", writer)
    diff.foreach(v => {
      writeLine(v.toString(), writer)
    })

    if (writer != null) {
      writer.close()
    }
  }

  def formatResultKey(fields:String): String = {
    fields.replaceAll(Constants.delimiter, Constants.delimRep)
  }

  def writeLine(line:String, writer:PrintWriter) = {
    if (writer != null) {
      writer.println(line.replaceAll(Constants.delimiter, Constants.delimRep))
    } else {
      println(line.replaceAll(Constants.delimiter, Constants.delimRep))
    }
  }

}