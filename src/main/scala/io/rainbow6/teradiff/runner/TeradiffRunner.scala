package io.rainbow6.teradiff.runner

import java.io.FileInputStream
import java.util.Properties

import io.rainbow6.teradiff.expression.ExpressionBuilder
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}
import org.apache.spark.SparkConf
import wpy.graphlinker.core.TeraCompare

object TeradiffRunner {

  def main(args:Array[String]): Unit = {

    if (args.length < 4) {
      System.err.println("Arguments: <source1> <source2> <sourceType: table/csv> <property file path> [partitions]")
      System.exit(1)
    }
    val source1 = args(0)
    val source2 = args(1)
    val sourceType = args(2)
    val propertyFilename = args(3)
    var partitions = 2000

    if (args.length > 4) {
      partitions = args(4).toInt
    }

    val conf = new SparkConf().setAppName("TeraDiff")//.setMaster("local")
    val spark = SparkSession.builder.config(conf).config("spark.sql.warehouse.dir", "/user/hive/warehouse/").enableHiveSupport().getOrCreate()
    spark.sql("set spark.sql.shuffle.partitions=%s".format(partitions))

    var df1 = null:DataFrame
    var df2 = null:DataFrame

    val properties = new Properties()
    properties.load(new FileInputStream(propertyFilename))
    val expression = new ExpressionBuilder(properties)

    val leftKeyExpr = expression.getLeftKeyExpr()
    val leftValueExpr = expression.getLeftValueExpr()
    val rightKeyExpr = expression.getRightKeyExpr()
    val rightValueExpr = expression.getRightValueExpr()

    if (sourceType == "table") {
      df1 = spark.read.table(source1)
      df2 = spark.read.table(source2)
    } else if (sourceType == "csv") {

      val leftSchema = expression.getLeftSchema()
      val rightSchema = expression.getRightSchema()

      df1 = spark.read.format("com.databricks.spark.csv")
        .option("header", false)
        .schema(leftSchema)
        .load(source1)

      df2 = spark.read.format("com.databricks.spark.csv")
        .option("header", false)
        .schema(rightSchema)
        .load(source2)
    } else {
      System.err.println("ERROR: Source type %s not supported".format(sourceType))
      System.exit(1)
    }

    // Execute spark job
    val compare = new TeraCompare(spark, df1, (leftKeyExpr, leftValueExpr), df2, (rightKeyExpr, rightValueExpr))

    val output = compare.compare()

    compare.analyzeResult(output)

  }
}
