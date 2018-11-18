package io.rainbow6.teradiff.expression

import java.util.Properties

import org.apache.spark.sql.types.{StringType, StructField, StructType}

import scala.collection.immutable.Map
import scala.collection.mutable.ListBuffer

class ExpressionBuilder(var leftKeyMap:Map[Int, String],
                        var leftValueMap:Map[Int, String],
                        var rightKeyMap:Map[Int, String],
                        var rightValueMap:Map[Int, String],
                        var leftKeyExpr:String,
                        var leftValueExpr:String,
                        var rightKeyExpr:String,
                        var rightValueExpr:String,
                        var leftSchema:StructType,
                        var rightSchema:StructType) {

  def this() {
    this(null, null, null, null, null, null, null, null, null, null)
  }

  def this(properties:Properties) {
    this(null, null, null, null, null, null, null, null, null, null)
    val leftKey = properties.getProperty("LEFT_KEY")
    val leftValue = properties.getProperty("LEFT_VALUES")
    val rightKey = properties.getProperty("RIGHT_KEY")
    val rightValue = properties.getProperty("RIGHT_VALUES")

    // Field Mapping
    leftKeyMap = getFieldMap(leftKey)
    leftValueMap = getFieldMap(leftValue)
    rightKeyMap = getFieldMap(rightKey)
    rightValueMap = getFieldMap(rightValue)

    // Expr
    leftKeyExpr = getExpression(leftKey, "key")
    leftValueExpr = getExpression(leftValue, "value")
    rightKeyExpr = getExpression(rightKey, "key")
    rightValueExpr = getExpression(rightValue, "value")

    // Schema
    leftSchema = getSchema(properties.getProperty("LEFT_SCHEMA"))
    rightSchema = getSchema(properties.getProperty("RIGHT_SCHEMA"))
  }

  def this(leftKey:String, leftValue:String, rightKey:String, rightValue:String) {
    this(null, null, null, null, null, null, null, null, null, null)

    // Field Mapping
    leftKeyMap = getFieldMap(leftKey)
    leftValueMap = getFieldMap(leftValue)
    rightKeyMap = getFieldMap(rightKey)
    rightValueMap = getFieldMap(rightValue)

    // Expr
    leftKeyExpr = getExpression(leftKey, "key")
    leftValueExpr = getExpression(leftValue, "value")
    rightKeyExpr = getExpression(rightKey, "key")
    rightValueExpr = getExpression(rightValue, "value")
  }

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

  def getFieldMap(fieldList:String):Map[Int, String] = {

    val list = new ListBuffer[(Int, String)]
    val fields = fieldList.split(",")
    for (i <- 0 to fields.length - 1) {
      list.append((i, fields(i)))
    }
    list.toMap
  }

  def getSchema(fieldList:String): StructType = {

    var schema = new StructType()
    fieldList.split(",").foreach(v => {
      schema = schema.add((StructField(v, StringType, true)))
    })

    schema
  }

  def getLeftKeyExpr(): String = {
    leftKeyExpr
  }

  def getLeftValueExpr(): String = {
    leftValueExpr
  }

  def getRightKeyExpr(): String = {
    rightKeyExpr
  }

  def getRightValueExpr(): String = {
    rightValueExpr
  }

  def getLeftSchema(): StructType = {
    leftSchema
  }

  def getRightSchema(): StructType = {
    rightSchema
  }

}
