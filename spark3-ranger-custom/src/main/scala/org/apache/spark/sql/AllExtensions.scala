package org.apache.spark.sql

import org.apache.spark.sql.extensions.RangerSparkSQLExtension


class AllExtensions extends ((SparkSessionExtensions) => Unit) {
  override def apply(ext: SparkSessionExtensions): Unit = {
    new RangerSparkSQLExtension()(ext)
  }
}