package org.apache.spark.sql

import org.apache.commons.logging.{Log, LogFactory}

object UserAuthUtils {
  val LOG: Log = LogFactory.getLog("org.apache.spark.sql.UserAuthUtils")

  def getUserAuth(spark: SparkSession): (String, Set[String]) = {
    (spark.conf.get("spark.sql.proxy-user"), Set())
  }
}
