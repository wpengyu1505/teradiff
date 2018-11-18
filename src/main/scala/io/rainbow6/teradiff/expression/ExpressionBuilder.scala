package io.rainbow6.teradiff.expression

import java.util.Properties

import org.apache.spark.sql.types.{StringType, StructField, StructType}

class ExpressionBuilder(properties:Properties) {

  val leftSchema = properties.getProperty("LEFT_SCHEMA")
  val rightSchema = properties.getProperty("RIGHT_SCHEMA")
  val leftKey = properties.getProperty("LEFT_KEY")
  val rightKey = properties.getProperty("RIGHT_KEY")
  val leftValue = properties.getProperty("LEFT_VALUES")
  val rightValue = properties.getProperty("RIGHT_VALUES")

  def getExpression(fieldList:String, columnName:String):String = {

    val cols = fieldList.split(",")

    val sb = new StringBuilder()
    sb.append("concat_ws('%s'".format(Constants.delimiter))
    cols.foreach(v => {
      sb.append(",%s".format(v))
    })
    sb.append(") as %s".format(columnName))

    sb.toString()
  }

  def getSchema(fieldList:String): StructType = {

    var schema = new StructType()
    fieldList.split(",").foreach(v => {
      schema = schema.add((StructField(v, StringType, true)))
    })

    schema
  }

  def getLeftKeyExpr(): String = {
    getExpression(leftKey, "key")
  }

  def getLeftValueExpr(): String = {
    getExpression(leftValue, "value")
  }

  def getRightKeyExpr(): String = {
    getExpression(rightKey, "key")
  }

  def getRightValueExpr(): String = {
    getExpression(rightValue, "value")
  }

  def getLeftSchema(): StructType = {
    getSchema(leftSchema)
  }

  def getRightSchema(): StructType = {
    getSchema(rightSchema)
  }

}
