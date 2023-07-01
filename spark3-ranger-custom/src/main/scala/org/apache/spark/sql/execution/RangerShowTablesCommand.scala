package org.apache.spark.sql.execution

import org.apache.ranger.authorization.spark.authorizer.{RangerSparkAuthorizer, SparkPrivilegeObject, SparkPrivilegeObjectType}
import org.apache.spark.sql.execution.command.{RunnableCommand, ShowTablesCommand}
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan

case class RangerShowTablesCommand(child: ShowTablesCommand) extends RunnableCommand {

  override val output: Seq[Attribute] = child.output
  override def run(sparkSession: SparkSession): Seq[Row] = {
    val rows = child.run(sparkSession)
    rows.filter(r => {
      println()
      r.getString(1) != null && RangerSparkAuthorizer.isAllowed(sparkSession,toSparkPrivilegeObject(r))
    })
  }

  private def toSparkPrivilegeObject(row: Row): SparkPrivilegeObject = {
    val database = row.getString(0)
    val table = row.getString(1)
    new SparkPrivilegeObject(SparkPrivilegeObjectType.TABLE_OR_VIEW, database, table)
  }

  /**
   * org.apache.spark.sql.catalyst.trees.TreeNode
   * Don't know if it work.
   */
  override protected def withNewChildrenInternal(newChildren: IndexedSeq[LogicalPlan]): RunnableCommand = {
//    copy(child = child.legacyWithNewChildren(newChildren).asInstanceOf[ShowTablesCommand])
    super.legacyWithNewChildren(newChildren).asInstanceOf[ShowTablesCommand]
  }
}
