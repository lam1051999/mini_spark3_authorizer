package org.apache.ranger.authorization.spark.authorizer

object SparkPrivObjectActionType extends Enumeration {
  type SparkPrivObjectActionType = Value
  val OTHER, INSERT, INSERT_OVERWRITE = Value
}
