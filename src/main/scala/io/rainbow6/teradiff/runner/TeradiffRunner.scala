package io.rainbow6.teradiff.runner

import java.io.FileInputStream
import java.io.PrintWriter
import java.util
import java.util.Properties

import io.rainbow6.teradiff.expression.ExpressionBuilder
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}
import org.apache.spark.SparkConf
import io.rainbow6.teradiff.core.TeraCompare
import org.apache.spark.sql.types.StructType
import org.kohsuke.args4j.{CmdLineParser, Option}

object TeradiffRunner {

  // Use args4j
  @Option(name = "--left", required = true, usage = "left source") var source1:String = null
  @Option(name = "--right", required = true, usage = "right source") var source2:String = null
  @Option(name = "--sourceType1", required = true, usage = "left type of data (rdbms/csv/hive/json/parquet)") var sourceType1:String = null
  @Option(name = "--sourceType2", required = true, usage = "right type of data (rdbms/csv/hive/json/parquet)") var sourceType2:String = null
  @Option(name = "--propertyFile", required = true, usage = "property file path") var propertyFilename:String = null
  @Option(name = "--outputFile", required = true, usage = "summary file path") var outputFile:String = null
  @Option(name = "--runMode", required = false, usage = "local or yarn") var runMode:String = "local"
  @Option(name = "--leftSchema", required = false, usage = "left data schema") var leftSchemaStr:String = ""
  @Option(name = "--rightSchema", required = false, usage = "right data schema") var rightSchemaStr:String = ""
  @Option(name = "--leftKey", required = false, usage = "left key fields") var leftKey:String = null
  @Option(name = "--leftValue", required = false, usage = "left value fields") var leftValue:String = null
  @Option(name = "--rightKey", required = false, usage = "right key fields") var rightKey:String = null
  @Option(name = "--rightValue", required = false, usage = "right value fields") var rightValue:String = null
  @Option(name = "--leftIgnores", required = false, usage = "left ignore fields") var leftIgnores:String = ""
  @Option(name = "--rightIgnores", required = false, usage = "right ignore fields") var rightIgnores:String = ""
  @Option(name = "--leftDelimiter", required = false, usage = "left csv delimiter") var leftDelimiter:String = ","
  @Option(name = "--rightDelimiter", required = false, usage = "right csv delimiter") var rightDelimiter:String = ","
  @Option(name = "--leftWithHeader", required = false, usage = "left csv with header") var leftWithHeader:Boolean = false
  @Option(name = "--rightWithHeader", required = false, usage = "right csv with header") var rightWithHeader:Boolean = false
  @Option(name = "--syncSchema", required = false, usage = "left or right") var syncSchema:String = null
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
      spark = SparkSession.builder.config(conf).config("spark.driver.bindAddress", "localhost").getOrCreate()
    } else {
      spark = SparkSession.builder.config(conf).config("spark.sql.warehouse.dir", "/user/hive/warehouse/").enableHiveSupport().getOrCreate()
    }
    spark.sql("set spark.sql.shuffle.partitions=%s".format(partitions))

    val properties = new Properties()
    properties.load(new FileInputStream(propertyFilename))

    val expression = new ExpressionBuilder(leftKey, leftValue, rightKey, rightValue,
      leftIgnores, rightIgnores, leftDelimiter, rightDelimiter, leftWithHeader, rightWithHeader)

    var df1 = getLeftDataFrame(spark, expression, properties)
    var df2 = getRightDataFrame(spark, expression, properties)
    val schema1 = df1.schema
    val schema2 = df2.schema

    // SyncSchema
    if (syncSchema != null) {
      if (syncSchema == "left") {
        df2 = getRightDataFrame(spark, expression, properties, schema1)
      } else if (syncSchema == "right") {
        df1 = getLeftDataFrame(spark, expression, properties, schema2)
      }
    }

    expression.analyze(df1, df2)

    // Execute spark job
    val compare = new TeraCompare(spark, expression, df1, df2)

    val output = compare.compare()

    val writer = new PrintWriter(outputFile)
    compare.writeLine("============= Sources ==============", writer)
    compare.writeLine("LHS (Left side): %s".format(source1), writer)
    compare.writeLine("RHS (Right side): %s".format(source2), writer)

    compare.writeLine("============= Ignores ==============", writer)
    compare.writeLine("LHS (Left side): %s".format(leftIgnores), writer)
    compare.writeLine("RHS (Right side): %s".format(rightIgnores), writer)

    compare.analyzeResult(output, writer)

  }

  def getLeftDataFrame(spark:SparkSession, expression:ExpressionBuilder, properties:Properties, schema: StructType = null): DataFrame = {
    if (sourceType1 == "hive") {
      spark.read.table(source1)
    } else if (sourceType1 == "csv") {

      val leftSchema = expression.getSchema(leftSchemaStr)
      val leftDelimiter = expression.getLeftDelimiter()

      if (expression.leftWithHeader()) {
        spark.read.format("com.databricks.spark.csv")
          .option("header", true)
          .option("delimiter", leftDelimiter)
          .load(source1)
      } else {
        spark.read.format("com.databricks.spark.csv")
          .option("header", false)
          .option("delimiter", leftDelimiter)
          .schema(leftSchema)
          .load(source1)
      }
    } else if (sourceType1 == "json") {
      if (schema == null) {
        spark.read.json(source1)
      } else {
        spark.read.schema(schema).json(source1)
      }
    } else if (sourceType1 == "parquet") {
      spark.read.parquet(source1)
    } else if (sourceType1 == "rdbms") {
      val leftConnectionProperties = new Properties()
      leftConnectionProperties.put("user", properties.getProperty("LEFT_USERNAME"))
      leftConnectionProperties.put("password", properties.getProperty("LEFT_PASSWORD"))
      leftConnectionProperties.setProperty("Driver", properties.getProperty("LEFT_DRIVER_CLASS"))
      spark.read.jdbc(properties.getProperty("LEFT_CONNECTION"), source1, leftConnectionProperties)
    } else {
      throw new RuntimeException(s"ERROR: Source type ${sourceType1} not supported")
    }
  }

  def getRightDataFrame(spark:SparkSession, expression:ExpressionBuilder, properties:Properties, schema: StructType = null): DataFrame = {
    if (sourceType2 == "hive") {
      spark.read.table(source2)
    } else if (sourceType2 == "csv") {

      val rightSchema = expression.getSchema(rightSchemaStr)
      val rightDelimiter = expression.getRightDelimiter()

      if (expression.rightWithHeader()) {
        spark.read.format("com.databricks.spark.csv")
          .option("header", true)
          .option("delimiter", rightDelimiter)
          .load(source2)
      } else {
        spark.read.format("com.databricks.spark.csv")
          .option("header", false)
          .option("delimiter", rightDelimiter)
          .schema(rightSchema)
          .load(source2)
      }
    } else if (sourceType2 == "json") {
      if (schema == null) {
        spark.read.json(source2)
      } else {
        spark.read.schema(schema).json(source2)
      }
    } else if (sourceType2 == "parquet") {
      spark.read.parquet(source2)
    } else if (sourceType2 == "rdbms") {
      val rightConnectionProperties = new Properties()
      rightConnectionProperties.put("user", properties.getProperty("RIGHT_USERNAME"))
      rightConnectionProperties.put("password", properties.getProperty("RIGHT_PASSWORD"))
      rightConnectionProperties.setProperty("Driver", properties.getProperty("RIGHT_DRIVER_CLASS"))
      spark.read.jdbc(properties.getProperty("RIGHT_CONNECTION"), source2, rightConnectionProperties)
    } else {
      throw new RuntimeException(s"ERROR: Source type ${sourceType2} not supported")
    }
  }
}
