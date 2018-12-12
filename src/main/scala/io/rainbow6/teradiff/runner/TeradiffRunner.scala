package io.rainbow6.teradiff.runner

import java.io.FileInputStream
import java.io.PrintWriter
import java.util.Properties

import io.rainbow6.teradiff.expression.ExpressionBuilder
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}
import org.apache.spark.SparkConf
import io.rainbow6.teradiff.core.TeraCompare

object TeradiffRunner {

  def main(args:Array[String]): Unit = {

    if (args.length < 6) {
      System.err.println("Arguments: <source1> <source2> <sourceType: table/csv> <property file path> <result file path> <run mode> [partitions]")
      System.exit(1)
    }
    val source1 = args(0)
    val source2 = args(1)
    val sourceType = args(2)
    val propertyFilename = args(3)
    val outputFile = args(4)
    val runMode = args(5)
    var partitions = 2000

    if (args.length > 6) {
      partitions = args(6).toInt
    }

    var spark = null:SparkSession
    val conf = new SparkConf().setAppName("TeraDiff")
    if (runMode == "local") {
      conf.setMaster("local")
      spark = SparkSession.builder.config(conf).getOrCreate()
    } else {
      spark = SparkSession.builder.config(conf).config("spark.sql.warehouse.dir", "/user/hive/warehouse/").enableHiveSupport().getOrCreate()
    }
    spark.sql("set spark.sql.shuffle.partitions=%s".format(partitions))

    var df1 = null:DataFrame
    var df2 = null:DataFrame

    val properties = new Properties()
    properties.load(new FileInputStream(propertyFilename))
    val expression = new ExpressionBuilder(properties)

    if (sourceType == "hive") {
      df1 = spark.read.table(source1)
      df2 = spark.read.table(source2)
    } else if (sourceType == "csv") {

      val leftSchema = expression.getLeftSchema()
      val rightSchema = expression.getRightSchema()
      val leftDelimiter = expression.getLeftDelimiter()
      val rightDelimiter = expression.getRightDelimiter()

      if (expression.leftWithHeader()) {
        df1 = spark.read.format("com.databricks.spark.csv")
          .option("header", true)
          .option("delimiter", leftDelimiter)
          .load(source1)
      } else {
        df1 = spark.read.format("com.databricks.spark.csv")
          .option("header", false)
          .option("delimiter", leftDelimiter)
          .schema(leftSchema)
          .load(source1)
      }

      if (expression.rightWithHeader()) {
        df2 = spark.read.format("com.databricks.spark.csv")
          .option("header", true)
          .option("delimiter", rightDelimiter)
          .load(source2)
      } else {
        df2 = spark.read.format("com.databricks.spark.csv")
          .option("header", false)
          .option("delimiter", rightDelimiter)
          .schema(rightSchema)
          .load(source2)
      }


    } else if (sourceType == "rdbms") {

      val leftConnectionProperties = new Properties()
      leftConnectionProperties.put("user", properties.getProperty("LEFT_USERNAME"))
      leftConnectionProperties.put("password", properties.getProperty("LEFT_PASSWORD"))
      leftConnectionProperties.setProperty("Driver", properties.getProperty("LEFT_DRIVER_CLASS"))
      df1 = spark.read.jdbc(properties.getProperty("LEFT_CONNECTION"), source1, leftConnectionProperties)

      val rightConnectionProperties = new Properties()
      rightConnectionProperties.put("user", properties.getProperty("RIGHT_USERNAME"))
      rightConnectionProperties.put("password", properties.getProperty("RIGHT_PASSWORD"))
      rightConnectionProperties.setProperty("Driver", properties.getProperty("RIGHT_DRIVER_CLASS"))
      df2 = spark.read.jdbc(properties.getProperty("RIGHT_CONNECTION"), source2, rightConnectionProperties)

      //Class.forName(properties.getProperty("LEFT_DRIVER_CLASS"))

    } else {
      System.err.println("ERROR: Source type %s not supported".format(sourceType))
      System.exit(1)
    }

    expression.analyze(df1, df2)

    // Execute spark job
    val compare = new TeraCompare(spark, expression, df1, df2)

    val output = compare.compare()

    val writer = new PrintWriter(outputFile)
    compare.analyzeResult(output, writer)

  }
}
