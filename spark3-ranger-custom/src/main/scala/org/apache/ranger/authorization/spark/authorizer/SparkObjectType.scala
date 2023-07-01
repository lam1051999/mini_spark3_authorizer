package org.apache.ranger.authorization.spark.authorizer

object SparkObjectType extends Enumeration {
  type SparkObjectType = Value

  val NONE, DATABASE, TABLE, VIEW, COLUMN, FUNCTION, URI = Value
}
