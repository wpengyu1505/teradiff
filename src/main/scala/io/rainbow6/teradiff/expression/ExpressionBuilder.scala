package io.rainbow6.teradiff.expression

import java.util.Properties

class ExpressionBuilder(properties:Properties) {

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

  def buildKeyExpr(propertyName:String): String = {
    getExpression(properties.getProperty(propertyName), "key")
  }

  def buildValueExpr(propertyName:String): String = {
    getExpression(properties.getProperty(propertyName), "value")
  }
}
