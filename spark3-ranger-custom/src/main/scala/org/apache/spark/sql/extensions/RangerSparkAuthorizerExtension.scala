package org.apache.spark.sql.extensions

import org.apache.commons.logging.LogFactory
import org.apache.ranger.authorization.spark.authorizer.{RangerSparkAuthorizer, RangerSparkPlugin, SparkAccessControlException, SparkOperationType}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.plans.logical.{Command, LogicalPlan}
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.execution.RangerShowTablesCommand
import org.apache.spark.sql.execution.command._
import org.apache.spark.sql.execution.datasources.{CreateTempViewUsing, InsertIntoDataSourceCommand, InsertIntoHadoopFsRelationCommand, SaveIntoDataSourceCommand}
import org.apache.spark.sql.hive.PrivilegesBuilder
import org.apache.spark.sql.hive.execution.{CreateHiveTableAsSelectCommand, InsertIntoHiveDirCommand, InsertIntoHiveTable}

case class RangerSparkAuthorizerExtension(spark: SparkSession) extends Rule[LogicalPlan] {

  import SparkOperationType._

  private val LOG = LogFactory.getLog(classOf[RangerSparkAuthorizerExtension])

  /**
   * Visit the [[LogicalPlan]] recursively to get all spark privilege objects, check the privileges
   *
   * If the user is authorized, then the original plan will be returned; otherwise, interrupted by
   * some particular privilege exceptions.
   *
   * @param plan a spark LogicalPlan for verifying privileges
   * @return a plan itself which has gone through the privilege check.
   */
  override def apply(plan: LogicalPlan): LogicalPlan = {
    plan match {
      case s: ShowTablesCommand => RangerShowTablesCommand(s)
      case s: SetCommand => {
        if (!s.kv.isEmpty && !s.kv.get._1.isEmpty) {
          if (RangerSparkPlugin.sparkSetExcludeKeys.exists(x => s.kv.get._1.matches(x))) {
            val e = new SparkAccessControlException(s"All user not allow to set this property: ${s.kv.get._1} !!!")
            LOG.error(
              s"""
                 |+===============================+
                 ||Spark SQL Authorization Failure|
                 ||-------------------------------|
                 ||${e.getMessage}
                 ||-------------------------------|
                 ||Spark SQL Authorization Failure|
                 |+===============================+
             """.stripMargin)
            throw e
          }
        }
        s
      }
      case r: RangerShowTablesCommand => r
      case _ =>
        // extract Operation Type
        val operationType: SparkOperationType = toOperationType(plan)
        // extract logic plan in, out
        val (in, out) = PrivilegesBuilder.build(plan)
        try {
          RangerSparkAuthorizer.checkPrivileges(spark, operationType, in, out)
          plan
        } catch {
          case ace: SparkAccessControlException =>
            LOG.error(
              s"""
                 |+===============================+
                 ||Spark SQL Authorization Failure|
                 ||-------------------------------|
                 ||${ace.getMessage}
                 ||-------------------------------|
                 ||Spark SQL Authorization Failure|
                 |+===============================+
             """.stripMargin)
            throw ace
        }
    }
  }

  /**
   * Mapping of [[LogicalPlan]] -> [[SparkOperationType]]
   *
   * @param plan a spark LogicalPlan
   * @return
   */
  private def toOperationType(plan: LogicalPlan): SparkOperationType = {
    plan match {
      case c: Command => c match {
        case _: AlterDatabasePropertiesCommand => ALTERDATABASE
        case _: AlterDatabaseSetLocationCommand => ALTERDATABASE_LOCATION
        case _: AlterTableAddColumnsCommand => ALTERTABLE_ADDCOLS
        case _: AlterTableAddPartitionCommand => ALTERTABLE_ADDPARTS
        case _: AlterTableChangeColumnCommand => ALTERTABLE_RENAMECOL
        case _: AlterTableDropPartitionCommand => ALTERTABLE_DROPPARTS
        case a: AlterTableRenameCommand => if (!a.isView) ALTERTABLE_RENAME else ALTERVIEW_RENAME
        case _: AlterTableRenamePartitionCommand => ALTERTABLE_RENAMEPART
        case _: AlterTableSerDePropertiesCommand => ALTERTABLE_SERDEPROPERTIES
        case _: AlterTableSetLocationCommand => ALTERTABLE_LOCATION
        case _: AlterTableSetPropertiesCommand
             | _: AlterTableUnsetPropertiesCommand => ALTERTABLE_PROPERTIES
        case _: AlterViewAsCommand => QUERY
        case _: AnalyzeColumnCommand => QUERY
        case _: AnalyzePartitionCommand => QUERY
        case _: AnalyzeTableCommand => QUERY
        case _: AnalyzeTablesCommand => QUERY
        case _: CreateDatabaseCommand => CREATEDATABASE
        case _: CreateDataSourceTableAsSelectCommand
             | _: CreateHiveTableAsSelectCommand => CREATETABLE_AS_SELECT
        case _: CreateTableCommand
             | _: CreateDataSourceTableCommand => CREATETABLE
        case _: CreateFunctionCommand => CREATEFUNCTION
        case _: CreateTableLikeCommand => CREATETABLE
        case _: CreateViewCommand
             | _: CreateTempViewUsing => CREATEVIEW
        case _: DescribeColumnCommand => DESCTABLE
        case _: DescribeDatabaseCommand => DESCDATABASE
        case _: DescribeFunctionCommand => DESCFUNCTION
        case _: DescribeQueryCommand => DESCQUERY
        case _: DescribeTableCommand => DESCTABLE
        case _: DropDatabaseCommand => DROPDATABASE
        // Hive don't check privileges for `drop function command`, what about a unverified user
        // try to drop functions.
        // We treat permanent functions as tables for verifying.
        case d: DropFunctionCommand if !d.isTemp => DROPTABLE
        case d: DropFunctionCommand if d.isTemp => DROPFUNCTION
        case d: DropTableCommand => if(!d.isView) DROPTABLE else DROPVIEW
        case e: ExplainCommand => toOperationType(e.logicalPlan)
        case _: InsertIntoDataSourceCommand => QUERY
        case _: InsertIntoDataSourceDirCommand => QUERY
        case _: InsertIntoHadoopFsRelationCommand => LOAD //CREATETABLE_AS_SELECT
        case _: InsertIntoHiveDirCommand => QUERY
        case _: InsertIntoHiveTable => LOAD
        case _: LoadDataCommand => LOAD
        case _: RefreshFunctionCommand => REFRESHFUNCTION
        case _: RefreshTableCommand => REFRESHTABLE
        case _: RepairTableCommand => MSCK
        case _: SaveIntoDataSourceCommand => QUERY
        case s: SetCommand if s.kv.isEmpty || s.kv.get._2.isEmpty => SHOWCONF
        case _: ShowColumnsCommand => SHOWCOLUMNS
        case _: ShowCreateTableAsSerdeCommand
             | _: ShowCreateTableCommand => SHOW_CREATETABLE
        case _: ShowFunctionsCommand => SHOWFUNCTIONS
        case _: ShowPartitionsCommand => SHOWPARTITIONS
        case _: ShowTablePropertiesCommand => SHOW_TBLPROPERTIES
        case _: ShowTablesCommand => SHOWTABLES
        case _: ShowViewsCommand => SHOWVIEWS
        case s: StreamingExplainCommand => toOperationType(s.queryExecution.optimizedPlan)
        case _: TruncateTableCommand => TRUNCATETABLE
        // Commands that do not need build privilege goes as explain type //
        case _ =>
          // AddFileCommand
          // AddJarCommand
          // ...
          EXPLAIN
      }
      case _ => QUERY
    }
  }

}
