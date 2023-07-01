package org.apache.spark.sql.hive


import org.apache.commons.logging.LogFactory
import org.apache.kudu.spark.kudu.KuduRelation
import org.apache.ranger.authorization.spark.authorizer.{SparkAccessControlException, SparkPrivObjectActionType, SparkPrivilegeObject, SparkPrivilegeObjectType}
import org.apache.ranger.authorization.spark.authorizer.SparkPrivObjectActionType.SparkPrivObjectActionType
import org.apache.spark.sql.SaveMode
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.analysis.{PersistedView, UnresolvedRelation}
import org.apache.spark.sql.catalyst.catalog.CatalogTable
import org.apache.spark.sql.catalyst.expressions.NamedExpression
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.execution.command._
import org.apache.spark.sql.execution.datasources._
import org.apache.spark.sql.AuthzUtils._
import org.apache.spark.sql.hive.execution.{CreateHiveTableAsSelectCommand, InsertIntoHiveDirCommand, InsertIntoHiveTable}

import scala.collection.mutable.ArrayBuffer

/**
 * [[LogicalPlan]] -> list of [[SparkPrivilegeObject]]s
 */
private[sql] object PrivilegesBuilder {
  private val LOG = LogFactory.getLog("org.apache.spark.sql.hive.PrivilegesBuilder")

  /**
   * Build input and output privilege objects from a Spark's [[LogicalPlan]]
   *
   * For [[ExplainCommand]]s, build its child.
   * For [[RunnableCommand]]s, build outputs if it has an target to write, build inputs for the
   * inside query if exists.
   *
   * For other queries, build inputs.
   *
   * @param plan A Spark [[LogicalPlan]]
   */
  def build(plan: LogicalPlan): (Seq[SparkPrivilegeObject], Seq[SparkPrivilegeObject]) = {

    def doBuild(plan: LogicalPlan): (Seq[SparkPrivilegeObject], Seq[SparkPrivilegeObject]) = {
      val inputObjs = new ArrayBuffer[SparkPrivilegeObject]
      val outputObjs = new ArrayBuffer[SparkPrivilegeObject]
      plan match {
        // RunnableCommand
        case cmd: Command => buildCommand(cmd, inputObjs, outputObjs)
        // Queries
        case other =>
          buildQuery(plan, inputObjs)
      }
      (inputObjs, outputObjs)
    }

    plan match {
      case e: ExplainCommand => doBuild(e.logicalPlan)
      case p => doBuild(p)
    }
  }

  /**
   * Build SparkPrivilegeObjects from Spark LogicalPlan
   *
   * @param plan             a Spark LogicalPlan used to generate SparkPrivilegeObjects
   * @param privilegeObjects input or output spark privilege object list
   * @param projectionList   Projection list after pruning
   */
  private def buildQuery(
                          plan: LogicalPlan,
                          privilegeObjects: ArrayBuffer[SparkPrivilegeObject],
                          projectionList: Seq[NamedExpression] = Nil): Unit = {

    /**
     * Columns in Projection take priority for column level privilege checking
     *
     * @param table catalogTable of a given relation
     */
    def mergeProjection(table: CatalogTable): Unit = {
      if (projectionList.isEmpty) {
        addTableOrViewLevelObjs(
          table.identifier,
          privilegeObjects,
          table.partitionColumnNames,
          table.schema.fieldNames)
      } else {
        addTableOrViewLevelObjs(
          table.identifier,
          privilegeObjects,
          table.partitionColumnNames.filter(projectionList.map(_.name).contains(_)),
          projectionList.map(_.name))
      }
    }

    plan match {
      case p: Project =>
        buildQuery(p.child, privilegeObjects, p.projectList)

      case h if h.nodeName == "HiveTableRelation" =>
        mergeProjection(getFieldVal(h, "tableMeta").asInstanceOf[CatalogTable])

      case m if m.nodeName == "MetastoreRelation" =>
        mergeProjection(getFieldVal(m, "catalogTable").asInstanceOf[CatalogTable])

      case l: LogicalRelation if l.catalogTable.nonEmpty =>
        mergeProjection(l.catalogTable.get)

      case l: LogicalRelation if l.relation.isInstanceOf[KuduRelation] =>
        val rel = l.relation.asInstanceOf[KuduRelation]
        val dbTbl = rel.tableName.split("::")(1).split("""\.""")
        addDBAndTableLevelObjs(
          Some(dbTbl(0)),
          dbTbl(1),
          privilegeObjects,
          Nil,
          if(projectionList.isEmpty) rel.schema.map(s => s.name) else projectionList.map(_.name)
        )
      case l: LogicalRelation => throw new SparkAccessControlException(s"User try access unsupport catalog, contact admin to get more info: ${l.treeString} !!!")
      case u: UnresolvedRelation =>
        // Normally, we shouldn't meet UnresolvedRelation here in an optimized plan.
        // Unfortunately, the real world is always a place where miracles happen.
        // We check the privileges directly without resolving the plan and leave everything
        // to spark to do.
        addTableOrViewLevelObjs(new TableIdentifier(u.tableName), privilegeObjects)

      case p =>
        for (child <- p.children) {
          buildQuery(child, privilegeObjects, projectionList)
        }
    }
  }

  /**
   * Build SparkPrivilegeObjects from Spark LogicalPlan
   *
   * @param plan       a Spark LogicalPlan used to generate SparkPrivilegeObjects
   * @param inputObjs  input spark privilege object list
   * @param outputObjs output spark privilege object list
   */
  private def buildCommand(
                            plan: LogicalPlan,
                            inputObjs: ArrayBuffer[SparkPrivilegeObject],
                            outputObjs: ArrayBuffer[SparkPrivilegeObject]): Unit = {
    plan match {
      case a: AlterDatabasePropertiesCommand => addDbLevelObjs(a.databaseName, outputObjs)

      case a: AlterDatabaseSetLocationCommand => addDbLevelObjs(a.databaseName, outputObjs)

      case a: AlterTableAddColumnsCommand =>
        addTableOrViewLevelObjs(
          a.table,
          inputObjs,
          columns = a.colsToAdd.map(_.name))
        addTableOrViewLevelObjs(
          a.table,
          outputObjs,
          columns = a.colsToAdd.map(_.name))

      case a: AlterTableAddPartitionCommand =>
        addTableOrViewLevelObjs(a.tableName, inputObjs)
        addTableOrViewLevelObjs(a.tableName, outputObjs)

      case a: RepairTableCommand =>
        addTableOrViewLevelObjs(a.tableName, inputObjs)
        addTableOrViewLevelObjs(a.tableName, outputObjs)

      case a: AlterTableChangeColumnCommand =>
        addTableOrViewLevelObjs(
          a.tableName,
          inputObjs,
          columns = Seq(a.columnName))

      case a: AlterTableDropPartitionCommand =>
        addTableOrViewLevelObjs(a.tableName, inputObjs)
        addTableOrViewLevelObjs(a.tableName, outputObjs)

      case a: AlterTableRenameCommand if !a.isView || a.oldName.database.nonEmpty =>
        // rename tables / permanent views
        addTableOrViewLevelObjs(a.oldName, inputObjs)
        addTableOrViewLevelObjs(a.newName, outputObjs)

      case a: AlterTableRenamePartitionCommand =>
        addTableOrViewLevelObjs(a.tableName, inputObjs)
        addTableOrViewLevelObjs(a.tableName, outputObjs)

      case a: AlterTableSerDePropertiesCommand =>
        addTableOrViewLevelObjs(a.tableName, inputObjs)
        addTableOrViewLevelObjs(a.tableName, outputObjs)

      case a: AlterTableSetLocationCommand =>
        addTableOrViewLevelObjs(a.tableName, inputObjs)
        addTableOrViewLevelObjs(a.tableName, outputObjs)

      case a: AlterTableSetPropertiesCommand =>
        addTableOrViewLevelObjs(a.tableName, inputObjs)
        addTableOrViewLevelObjs(a.tableName, outputObjs)

      case a: AlterTableUnsetPropertiesCommand =>
        addTableOrViewLevelObjs(a.tableName, inputObjs)
        addTableOrViewLevelObjs(a.tableName, outputObjs)

      case a: AlterViewAsCommand =>
        if (a.name.database.nonEmpty) {
          // it's a permanent view
          addTableOrViewLevelObjs(a.name, outputObjs)
        }
        buildQuery(a.query, inputObjs)

      case a: AnalyzeColumnCommand =>
        addTableOrViewLevelObjs(
          a.tableIdent, inputObjs, columns = a.columnNames.get)
        addTableOrViewLevelObjs(
          a.tableIdent, outputObjs, columns = a.columnNames.get)

      case a: AnalyzePartitionCommand =>
        addTableOrViewLevelObjs(a.tableIdent, inputObjs)
        addTableOrViewLevelObjs(a.tableIdent, outputObjs)

      case a: AnalyzeTableCommand =>
        addTableOrViewLevelObjs(a.tableIdent, inputObjs, columns = Seq("RAW__DATA__SIZE"))
        addTableOrViewLevelObjs(a.tableIdent, outputObjs)

      case a: AnalyzeTablesCommand =>
        addDbLevelObjs(a.databaseName, inputObjs)
        addDbLevelObjs(a.databaseName, outputObjs)

      case c: CreateDatabaseCommand => addDbLevelObjs(c.databaseName, outputObjs)

      case c: CreateDataSourceTableAsSelectCommand =>
        addDbLevelObjs(c.table.identifier, outputObjs)
        addTableOrViewLevelObjs(c.table.identifier, outputObjs, mode = c.mode)
        buildQuery(c.query, inputObjs)

      case c: CreateDataSourceTableCommand =>
        addTableOrViewLevelObjs(c.table.identifier, outputObjs)

      case c: CreateFunctionCommand if !c.isTemp =>
        addDbLevelObjs(c.databaseName, outputObjs)
        addFunctionLevelObjs(c.databaseName, c.functionName, outputObjs)

      case c: CreateHiveTableAsSelectCommand =>
        addDbLevelObjs(c.tableDesc.identifier, outputObjs)
        addTableOrViewLevelObjs(c.tableDesc.identifier, outputObjs)
        buildQuery(c.query, inputObjs)

      case c: CreateTableCommand =>
        addDbLevelObjs(c.table.database, outputObjs)
        addTableOrViewLevelObjs(c.table.identifier, outputObjs)

      case c: CreateTableLikeCommand =>
        addDbLevelObjs(c.targetTable, outputObjs)
        addTableOrViewLevelObjs(c.targetTable, outputObjs)
        // hive don't handle source table's privileges, we should not obey that, because
        // it will cause meta information leak
        addDbLevelObjs(c.sourceTable, inputObjs)
        addTableOrViewLevelObjs(c.sourceTable, inputObjs)

      case c: CreateViewCommand =>
        c.viewType match {
          case PersistedView =>
            // PersistedView will be tied to a database
            addDbLevelObjs(c.name, outputObjs)
            addTableOrViewLevelObjs(c.name, outputObjs)
          case _ =>
        }
        buildQuery(c.plan, inputObjs)

      case d: DescribeColumnCommand =>
        addTableOrViewLevelObjs(
          d.table,
          inputObjs,
          columns = d.colNameParts)

      case d: DescribeDatabaseCommand =>
        addDbLevelObjs(d.databaseName, inputObjs)

      case d: DescribeQueryCommand =>
        buildQuery(d.plan, inputObjs)

      case d: DescribeFunctionCommand =>
        addFunctionLevelObjs(Option(d.info.getDb), d.info.getName, inputObjs)

      case d: DescribeTableCommand => addTableOrViewLevelObjs(d.table, inputObjs)

      case d: DropDatabaseCommand =>
        addDbLevelObjs(d.databaseName, inputObjs)
        addDbLevelObjs(d.databaseName, outputObjs)

      case d: DropFunctionCommand =>
        addFunctionLevelObjs(d.databaseName, d.functionName, inputObjs)
        addFunctionLevelObjs(d.databaseName, d.functionName, outputObjs)

      case d: DropTableCommand =>
        addTableOrViewLevelObjs(d.tableName, inputObjs)
        addTableOrViewLevelObjs(d.tableName, outputObjs)

      case i: InsertIntoDataSourceCommand =>
        i.logicalRelation.catalogTable.foreach { table =>
          addTableOrViewLevelObjs(
            table.identifier,
            outputObjs)
        }
        buildQuery(i.query, inputObjs)

      case i: InsertIntoDataSourceDirCommand =>
        buildQuery(i.query, inputObjs)

      case i: InsertIntoHadoopFsRelationCommand =>
        // we are able to get the override mode here, but ctas for hive table with text/orc
        // format and parquet with spark.sql.hive.convertMetastoreParquet=false can success
        // with privilege checking without claiming for UPDATE privilege of target table,
        // which seems to be same with Hive behaviour.
        // So, here we ignore the overwrite mode for such a consistency.
        i.catalogTable foreach { t =>
          addTableOrViewLevelObjs(
            t.identifier,
            outputObjs,
            i.partitionColumns.map(_.name),
            t.schema.fieldNames)
        }
        buildQuery(i.query, inputObjs)

      case i: InsertIntoHiveDirCommand =>
        buildQuery(i.query, inputObjs)

      case i: InsertIntoHiveTable =>
        addTableOrViewLevelObjs(i.table.identifier, outputObjs)
        buildQuery(i.query, inputObjs)

      case l: LoadDataCommand =>
        addTableOrViewLevelObjs(l.table, outputObjs)
        if (!l.isLocal) {
          inputObjs += new SparkPrivilegeObject(SparkPrivilegeObjectType.DFS_URI, l.path, l.path)
        }

      case s : SaveIntoDataSourceCommand =>
        buildQuery(s.query, outputObjs)

      case s: ShowColumnsCommand => addTableOrViewLevelObjs(s.tableName, inputObjs)

      case s: ShowCreateTableCommand => addTableOrViewLevelObjs(s.table, inputObjs)

      case s: ShowCreateTableAsSerdeCommand => addTableOrViewLevelObjs(s.table, inputObjs)

      case s: ShowFunctionsCommand => addDbLevelObjs(s.db, inputObjs)

      case s: ShowPartitionsCommand => addTableOrViewLevelObjs(s.tableName, inputObjs)

      case s: ShowTablePropertiesCommand => addTableOrViewLevelObjs(s.table, inputObjs)

      case s: ShowTablesCommand => addDbLevelObjs(s.databaseName, inputObjs)

      case s: ShowViewsCommand => addDbLevelObjs(s.databaseName, inputObjs)

      case s: TruncateTableCommand =>
        addTableOrViewLevelObjs(s.tableName, inputObjs)
        addTableOrViewLevelObjs(s.tableName, outputObjs)

      case s: RefreshTableCommand =>
        addTableOrViewLevelObjs(s.tableIdent, inputObjs)
        addTableOrViewLevelObjs(s.tableIdent, outputObjs)

      case c: RefreshFunctionCommand =>
        addFunctionLevelObjs(c.databaseName, c.functionName, inputObjs)
        addFunctionLevelObjs(c.databaseName, c.functionName, outputObjs)

      case _ =>
    }
  }

  /**
   * Add database level spark privilege objects to input or output list
   *
   * @param dbName           database name as spark privilege object
   * @param privilegeObjects input or output list
   */
  private def addDbLevelObjs(
                              dbName: String,
                              privilegeObjects: ArrayBuffer[SparkPrivilegeObject]): Unit = {
    privilegeObjects += new SparkPrivilegeObject(SparkPrivilegeObjectType.DATABASE, dbName, dbName)
  }

  /**
   * Add database level spark privilege objects to input or output list
   *
   * @param dbOption         an option of database name as spark privilege object
   * @param privilegeObjects input or output spark privilege object list
   */
  private def addDbLevelObjs(
                              dbOption: Option[String],
                              privilegeObjects: ArrayBuffer[SparkPrivilegeObject]): Unit = {
    dbOption match {
      case Some(db) =>
        privilegeObjects += new SparkPrivilegeObject(SparkPrivilegeObjectType.DATABASE, db, db)
      case _ =>
    }
  }

  /**
   * Add database level spark privilege objects to input or output list
   *
   * @param identifier       table identifier contains database name as hive privilege object
   * @param privilegeObjects input or output spark privilege object list
   */
  private def addDbLevelObjs(
                              identifier: TableIdentifier,
                              privilegeObjects: ArrayBuffer[SparkPrivilegeObject]): Unit = {
    identifier.database match {
      case Some(db) =>
        privilegeObjects += new SparkPrivilegeObject(SparkPrivilegeObjectType.DATABASE, db, db)
      case _ =>
    }
  }

  /**
   * Add function level spark privilege objects to input or output list
   *
   * @param databaseName     database name
   * @param functionName     function name as spark privilege object
   * @param privilegeObjects input or output list
   */
  private def addFunctionLevelObjs(
                                    databaseName: Option[String],
                                    functionName: String,
                                    privilegeObjects: ArrayBuffer[SparkPrivilegeObject]): Unit = {
    databaseName match {
      case Some(db) =>
        privilegeObjects += new SparkPrivilegeObject(
          SparkPrivilegeObjectType.FUNCTION, db, functionName)
      case _ =>
    }
  }

  /**
   * Add table level spark privilege objects to input or output list
   *
   * @param identifier       table identifier contains database name, and table name as hive
   *                         privilege object
   * @param privilegeObjects input or output list
   * @param mode             Append or overwrite
   */
  private def addTableOrViewLevelObjs(identifier: TableIdentifier,
                                      privilegeObjects: ArrayBuffer[SparkPrivilegeObject], partKeys: Seq[String] = Nil,
                                      columns: Seq[String] = Nil, mode: SaveMode = SaveMode.ErrorIfExists): Unit = {
    identifier.database match {
      case Some(db) =>
        val tbName = identifier.table
        val actionType = toActionType(mode)
        privilegeObjects += new SparkPrivilegeObject(
          SparkPrivilegeObjectType.TABLE_OR_VIEW,
          db,
          tbName,
          partKeys,
          columns,
          actionType)
      case _ =>
    }
  }

  /** **
   *
   * @param dbName
   * @param tableName
   * @param privilegeObjects
   */
  private def addDBAndTableLevelObjs(dbName: Option[String], tableName: String,
                                     privilegeObjects: ArrayBuffer[SparkPrivilegeObject], partKeys: Seq[String] = Nil,
                                     columns: Seq[String] = Nil, mode: SaveMode = SaveMode.ErrorIfExists): Unit = {
    dbName match {
      case Some(db) =>
        val actionType = toActionType(mode)
        privilegeObjects += new SparkPrivilegeObject(
          SparkPrivilegeObjectType.TABLE_OR_VIEW,
          db,
          tableName,
          partKeys,
          columns,
          actionType)
      case _ =>
    }

  }


  /**
   * SparkPrivObjectActionType INSERT or INSERT_OVERWRITE
   *
   * @param mode Append or Overwrite
   */
  private def toActionType(mode: SaveMode): SparkPrivObjectActionType = {
    mode match {
      case SaveMode.Append => SparkPrivObjectActionType.INSERT
      case SaveMode.Overwrite => SparkPrivObjectActionType.INSERT_OVERWRITE
      case _ => SparkPrivObjectActionType.OTHER
    }
  }
}
