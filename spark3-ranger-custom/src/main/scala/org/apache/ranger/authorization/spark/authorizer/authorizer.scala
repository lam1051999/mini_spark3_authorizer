package org.apache.ranger.authorization.spark

import org.apache.spark.sql.SparkSessionExtensions

package object authorizer {

  type Extensions = SparkSessionExtensions => Unit

}
