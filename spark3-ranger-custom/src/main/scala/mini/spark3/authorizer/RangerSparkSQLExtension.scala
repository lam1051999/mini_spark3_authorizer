package mini.spark3.authorizer

import org.apache.ranger.authorization.spark.authorizer.Extensions
import org.apache.spark.sql.SparkSessionExtensions
import org.apache.spark.sql.extensions.RangerSparkAuthorizerExtension

class RangerSparkSQLExtension extends Extensions {
  override def apply(ext: SparkSessionExtensions): Unit = {
    ext.injectOptimizerRule(RangerSparkAuthorizerExtension)
  }
}
