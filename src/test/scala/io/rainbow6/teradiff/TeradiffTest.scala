package io.rainbow6.teradiff

import io.rainbow6.teradiff.expression.ExpressionBuilder
import org.junit.Test
import junit.framework.TestCase
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.junit.Assert._
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.apache.spark.sql.Row
import io.rainbow6.teradiff.core.TeraCompare;

class TeradiffTest extends TestCase {

  //@Test
  def test(): Unit = {
    val conf = new SparkConf().setAppName("TeraDiff").setMaster("local")
    val spark = SparkSession.builder.config(conf).getOrCreate()

    // Low partition for local
    spark.sql("set spark.sql.shuffle.partitions=1")

    val schema = new StructType()
      .add(StructField("id", StringType, true))
      .add(StructField("col1", StringType, true))
      .add(StructField("col2", StringType, true))
      .add(StructField("col3", StringType, true))

    val df1 = spark.read.format("com.databricks.spark.csv")
      .option("header", false)
      .schema(schema)
      .load("src/test/resources/data1.txt")

    val df2 = spark.read.format("com.databricks.spark.csv")
      .option("header", false)
      .schema(schema)
      .load("src/test/resources/data2.txt")

    val keyExpr1 = "id as key"
    val valueExpr1 = "concat_ws('%s', col1, col2, col3) as value".format("\001")

    val compare = new TeraCompare(spark, df1, (keyExpr1, valueExpr1), df2, (keyExpr1, valueExpr1))

    val output = compare.compare()

    compare.analyzeResult(output, null)
  }

  @Test
  def testExpression() = {
    val builder = new ExpressionBuilder(null)
    val fields = "time,symbol,sequence,price"
    assertEquals("concat_ws('%s',time,symbol,sequence,price) as key".format('\001'), builder.getExpression(fields, "key"))
  }
}
