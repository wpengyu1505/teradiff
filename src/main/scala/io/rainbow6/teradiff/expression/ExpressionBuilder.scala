package io.rainbow6.teradiff.expression

import java.util.Properties

import org.apache.spark.sql.DataFrame
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
                        var leftIgnores:String,
                        var rightIgnores:String,
                        var leftSchema:StructType,
                        var rightSchema:StructType,
                        var leftDelimiter:String,
                        var rightDelimiter:String,
                        var leftHeader:Boolean,
                        var rightHeader:Boolean) extends Serializable {

  def this() {
    this(null, null, null, null, null, null, null, null, null, null, null, null, null, null, false, false)
  }

  def this(leftKeyExpr:String,
           leftValueExpr:String,
           rightKeyExpr:String,
           rightValueExpr:String,
           leftIgnores:String,
           rightIgnores:String,
           leftDelimiter:String,
           rightDelimiter:String,
           leftHeader:Boolean,
           rightHeader:Boolean) {
    this(null, null, null, null,
      leftKeyExpr, leftValueExpr, rightKeyExpr, rightValueExpr,
      leftIgnores, rightIgnores, null, null,
      leftDelimiter, rightDelimiter, leftHeader, rightHeader)

    this.leftIgnores = leftIgnores
    this.rightIgnores = rightIgnores

    // Delimiter default is comma, only user can overwrite using properties
    this.leftDelimiter = leftDelimiter
    this.rightDelimiter = rightDelimiter

    this.leftHeader = leftHeader
    this.rightHeader = rightHeader
  }

//  def this(leftKey:String, leftValue:String, rightKey:String, rightValue:String) {
//    this(null, null, null, null, null, null, null, null, null, null, null, null, null, null, false, false)
//
//    // Field Mapping
//    leftKeyMap = getFieldMap(leftKey, leftIgnores)
//    leftValueMap = getFieldMap(leftValue, leftIgnores)
//    rightKeyMap = getFieldMap(rightKey, rightIgnores)
//    rightValueMap = getFieldMap(rightValue, rightIgnores)
//
//    // Expr
//    leftKeyExpr = getExpression(leftKey, leftIgnores, "key")
//    leftValueExpr = getExpression(leftValue, leftIgnores, "value")
//    rightKeyExpr = getExpression(rightKey, rightIgnores, "key")
//    rightValueExpr = getExpression(rightValue, rightIgnores, "value")
//  }

//  def getExpression(fieldList:String, columnName:String):String = {
//
//    val cols = fieldList.split(",")
//
//    val sb = new StringBuilder()
//    sb.append("concat_ws('%s'".format(Constants.delimiter))
//    cols.foreach(v => {
//      sb.append(",%s".format(v))
//    })
//    sb.append(") as %s".format(columnName))
//
//    sb.toString()
//  }

  def getExpression(fieldList:String, ignoreList:String, columnName:String):String = {

    val cols = fieldList.split(",")
    val ignores = ignoreList.split(",")

    val sb = new StringBuilder()
    sb.append("concat_ws('%s'".format(Constants.delimiter))
    cols.foreach(v => {
      if (!ignores.contains(v)) {
        sb.append(",%s".format(v))
      }
    })
    sb.append(") as %s".format(columnName))

    sb.toString()
  }

  def getFieldMap(fieldList:String, ignoreList:String):Map[Int, String] = {

    val list = new ListBuffer[(Int, String)]
    val fields = fieldList.split(",")
    val ignores = ignoreList.split(",")
    var index = 0
    fields.foreach(v => {
      if (!ignores.contains(v)) {
        list.append((index, v))
        index += 1
      }
    })
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

    this.leftSchema = df1.schema
    this.rightSchema = df2.schema

    if (leftKeyExpr == null || leftKeyExpr.isEmpty) {
      leftKeyExpr = schemaToString(df1.schema)
    }
    if (leftValueExpr == null || leftValueExpr.isEmpty) {
      leftValueExpr = schemaToString(df1.schema)
    }
    if (rightKeyExpr == null || rightKeyExpr.isEmpty) {
      rightKeyExpr = schemaToString(df2.schema)
    }
    if (rightValueExpr == null || rightValueExpr.isEmpty) {
      rightValueExpr = schemaToString(df2.schema)
    }

    // Field Mapping
    leftKeyMap = getFieldMap(leftKeyExpr, leftIgnores)
    leftValueMap = getFieldMap(leftValueExpr, leftIgnores)
    rightKeyMap = getFieldMap(rightKeyExpr, rightIgnores)
    rightValueMap = getFieldMap(rightValueExpr, rightIgnores)

    // Expr
    leftKeyExpr = getExpression(leftKeyExpr, leftIgnores, "key")
    leftValueExpr = getExpression(leftValueExpr, leftIgnores, "value")
    rightKeyExpr = getExpression(rightKeyExpr, rightIgnores, "key")
    rightValueExpr = getExpression(rightValueExpr, rightIgnores, "value")

    println("=======================================")
    println("leftKey: %s".format(leftKeyExpr))
    println("leftValue: %s".format(leftValueExpr))
    println("rightKey: %s".format(rightKeyExpr))
    println("rightValue: %s".format(rightValueExpr))
    println("leftIgnores: %s".format(leftIgnores))
    println("rightIgnores %s".format(rightIgnores))
    println("=======================================")
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
