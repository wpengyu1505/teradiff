package io.rainbow6.teradiff.runner

import java.io.FileInputStream
import java.io.PrintWriter
import java.util
import java.util.Properties

import io.rainbow6.teradiff.expression.ExpressionBuilder
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}
import org.apache.spark.SparkConf
import io.rainbow6.teradiff.core.TeraCompare
import org.kohsuke.args4j.{CmdLineParser, Option}

object TeradiffRunner {

  // Use args4j
  @Option(name = "--left", required = true, usage = "left source") var source1:String = null
  @Option(name = "--right", required = true, usage = "right source") var source2:String = null
  @Option(name = "--sourceType", required = true, usage = "type of data (rdbms/csv/hive)") var sourceType:String = null
  @Option(name = "--propertyFile", required = true, usage = "property file path") var propertyFilename:String = null
  @Option(name = "--outputFile", required = true, usage = "summary file path") var outputFile:String = null
  @Option(name = "--runMode", required = false, usage = "local or yarn") var runMode:String = "local"
  @Option(name = "--leftSchema", required = false, usage = "left data schema") var leftSchema:String = null
  @Option(name = "--rightSchema", required = false, usage = "right data schema") var rightSchema:String = null
  @Option(name = "--leftKey", required = false, usage = "left key fields") var leftKey:String = null
  @Option(name = "--leftValue", required = false, usage = "left value fields") var leftValue:String = null
  @Option(name = "--rightKey", required = false, usage = "right key fields") var rightKey:String = null
  @Option(name = "--rightValue", required = false, usage = "right value fields") var rightValue:String = null
  @Option(name = "--leftDelimiter", required = false, usage = "left csv delimiter") var leftDelimiter:String = ","
  @Option(name = "--rightDelimiter", required = false, usage = "right csv delimiter") var rightDelimiter:String = ","
  @Option(name = "--leftWithHeader", required = false, usage = "left csv with header") var leftWithHeader:Boolean = false
  @Option(name = "--rightWithHeader", required = false, usage = "right csv with header") var rightWithHeader:Boolean = false
  @Option(name = "--partitions", required = false, usage = "num of partitions") var partitions:Int = 1

  def main(args:Array[String]): Unit = {

    var list = new util.ArrayList[String]
    args.foreach(v => {
      list.add(v)
    })
    val parser = new CmdLineParser(this)
    parser.parseArgument(list)

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

    val expression = new ExpressionBuilder(leftKey, leftValue, rightKey, rightValue,
      leftDelimiter, rightDelimiter, leftWithHeader, rightWithHeader)

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
