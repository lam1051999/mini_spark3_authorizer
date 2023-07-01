package org.apache.spark.sql

import scala.util.{Failure, Success, Try}

private[sql] object AuthzUtils {

  def getFieldVal(o: Any, name: String): Any = {
    Try {
      val field = o.getClass.getDeclaredField(name)
      field.setAccessible(true)
      field.get(o)
    } match {
      case Success(value) => value
      case Failure(exception) => throw exception
    }
  }
}
