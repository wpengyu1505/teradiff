package io.rainbow6.teradiff.core.model

import scala.collection.immutable.HashMap

case class ComparisonResult(
  key: String,
  result: String,
  recType: String
) {

  override def toString(): String = {
    "KEY: %s, RESULT: %s".format(key, result)
  }
}