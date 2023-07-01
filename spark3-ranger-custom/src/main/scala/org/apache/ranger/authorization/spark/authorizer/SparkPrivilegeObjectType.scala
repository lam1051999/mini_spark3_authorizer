package org.apache.ranger.authorization.spark.authorizer

object SparkPrivilegeObjectType extends Enumeration {
  type SparkPrivilegeObjectType = Value
  val DATABASE, TABLE_OR_VIEW, FUNCTION, DFS_URI = Value
}
