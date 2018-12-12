package io.rainbow6.teradiff.expression

import java.util.Properties

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.types.{StringType, StructField, StructType}

import scala.collection.immutable.Map
import scala.collection.mutable.ListBuffer

class ExpressionBuilder(var props:Properties,
                        var leftKeyMap:Map[Int, String],
                        var leftValueMap:Map[Int, String],
                        var rightKeyMap:Map[Int, String],
                        var rightValueMap:Map[Int, String],
                        var leftKeyExpr:String,
                        var leftValueExpr:String,
                        var rightKeyExpr:String,
                        var rightValueExpr:String,
                        var leftSchema:StructType,
                        var rightSchema:StructType,
                        var leftDelimiter:String,
                        var rightDelimiter:String,
                        var leftHeader:Boolean,
                        var rightHeader:Boolean) extends Serializable {

  def this() {
    this(null, null, null, null, null, null, null, null, null, null, null, null, null, false, false)
  }

  def this(properties:Properties) {
    this(null, null, null, null, null, null, null, null, null, null, null, null, null, false, false)
    props = properties
//    val leftKey = properties.getProperty("LEFT_KEY")
//    val leftValue = properties.getProperty("LEFT_VALUES")
//    val rightKey = properties.getProperty("RIGHT_KEY")
//    val rightValue = properties.getProperty("RIGHT_VALUES")
//
//    // Field Mapping
//    leftKeyMap = getFieldMap(leftKey)
//    leftValueMap = getFieldMap(leftValue)
//    rightKeyMap = getFieldMap(rightKey)
//    rightValueMap = getFieldMap(rightValue)
//
//    // Expr
//    leftKeyExpr = getExpression(leftKey, "key")
//    leftValueExpr = getExpression(leftValue, "value")
//    rightKeyExpr = getExpression(rightKey, "key")
//    rightValueExpr = getExpression(rightValue, "value")

    // Schema
    leftSchema = getSchema(properties.getProperty("LEFT_SCHEMA"))
    rightSchema = getSchema(properties.getProperty("RIGHT_SCHEMA"))

    // Delimiter default is comma, only user can overwrite using properties
    leftDelimiter = properties.getProperty("LEFT_DELIMITER")
    if (leftDelimiter.trim.isEmpty) {
      leftDelimiter = ","
    }
    rightDelimiter = properties.getProperty("RIGHT_DELIMITER")
    if (rightDelimiter.trim.isEmpty) {
      rightDelimiter = ","
    }

    if (properties.getProperty("LEFT_WITH_HEADER").toUpperCase == "Y") {
      leftHeader = true
    } else {
      leftHeader = false
    }

    if (properties.getProperty("RIGHT_WITH_HEADER").toUpperCase == "Y") {
      rightHeader = true
    } else {
      rightHeader = false
    }

  }

  def this(leftKey:String, leftValue:String, rightKey:String, rightValue:String) {
    this(null, null, null, null, null, null, null, null, null, null, null, null, null, false, false)

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

  def schemaToString(schema:StructType):String = {
    schema.fieldNames.mkString(",")
  }

  def analyze(df1:DataFrame, df2:DataFrame) = {
    var leftKey = props.getProperty("LEFT_KEY")
    var leftValue = props.getProperty("LEFT_VALUES")
    var rightKey = props.getProperty("RIGHT_KEY")
    var rightValue = props.getProperty("RIGHT_VALUES")

    if (leftKey == null || leftKey.isEmpty) {
      leftKey = schemaToString(df1.schema)
    }
    if (leftValue == null || leftValue.isEmpty) {
      leftValue = schemaToString(df1.schema)
    }
    if (rightKey == null || rightKey.isEmpty) {
      rightKey = schemaToString(df2.schema)
    }
    if (rightValue == null || rightValue.isEmpty) {
      rightValue = schemaToString(df2.schema)
    }

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

  def getColumnMap(): Map[Int, String] = {
    leftValueMap
  }

  def getLeftDelimiter(): String = {
    leftDelimiter
  }

  def getRightDelimiter(): String = {
    rightDelimiter
  }

  def leftWithHeader(): Boolean = {
    leftHeader
  }

  def rightWithHeader(): Boolean = {
    rightHeader
  }

}
