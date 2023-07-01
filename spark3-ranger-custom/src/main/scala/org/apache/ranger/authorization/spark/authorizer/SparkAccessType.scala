package org.apache.ranger.authorization.spark.authorizer

object SparkAccessType extends Enumeration {
  type SparkAccessType = Value

  val NONE, CREATE, ALTER, DROP, SELECT, UPDATE, USE, READ, WRITE, ALL, ADMIN = Value
}

