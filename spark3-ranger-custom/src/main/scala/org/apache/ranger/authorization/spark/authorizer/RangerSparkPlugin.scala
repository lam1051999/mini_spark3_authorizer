/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ranger.authorization.spark.authorizer

import java.io.{File, FileNotFoundException}
import java.util.Calendar

import org.apache.commons.logging.LogFactory
import org.apache.ranger.authorization.hadoop.config.RangerConfiguration
import org.apache.ranger.plugin.service.RangerBasePlugin

import scala.collection.mutable

class RangerSparkPlugin private extends RangerBasePlugin("spark", "ranger_customized") {

  import RangerSparkPlugin._

  private val LOG = LogFactory.getLog(classOf[RangerSparkPlugin])
  private var sparkSetExcludeKeys: mutable.HashSet[String] = null
  private val SPARK_SET_EXCLUDE_KEY_DEFAULT = "spark.sql.optimizer.excludedRules,spark.sql.proxy-user"

  lazy val fsScheme: Array[String] = RangerConfiguration.getInstance()
    .get("ranger.plugin.spark.urlauth.filesystem.schemes", "hdfs:,file:")
    .split(",")
    .map(_.trim)

  override def init(): Unit = {
    // must be init before using any config
    super.init()
    LOG.info(
      s"""
         |+===============================+
         ||Ranger Spark SQL Plugin Init   |
         ||-------------------------------|
         ||"${Calendar.getInstance().getTime} - App ID: ${this.getAppId} - Cluster Name: ${this.getClusterName} - Serice Name: ${this.getServiceName}"
         ||-------------------------------|
         ||Ranger Spark SQL Plugin Init   |
         |+===============================+
             """.stripMargin)
    if (sparkSetExcludeKeys == null) {
      sparkSetExcludeKeys = new mutable.HashSet[String]() ++ (rangerConf
        .get("ranger.plugin.spark.command.set.excludes", "spark.sql.optimizer.excludedRules")
        .split(",").map(x => x.trim)) ++ SPARK_SET_EXCLUDE_KEY_DEFAULT.split(",").map(x => x.trim)
    }
  }

  private def checkActive(): Unit = {
    val cacheFile = new File(rangerConf.get("ranger.plugin.spark.policy.cache.dir") + File.separatorChar
      + getAppId() + "_" + rangerConf.get("ranger.plugin.spark.service.name") + ".json")
    LOG.info(
      s"""
         |+===============================+
         ||Ranger Spark SQL policy file   |
         ||-------------------------------|
         ||"${cacheFile.toString}"
         ||-------------------------------|
         ||Ranger Spark SQL policy file   |
         |+===============================+
             """.stripMargin)

    if (!cacheFile.exists()) {
      throw new FileNotFoundException("Unable to find ranger policy cache directory at" +
        cacheFile.getAbsolutePath + ", file need to be exist at start to prevent Ranger host unreachable !!!")
    }
  }

  def getOrCreate(): RangerSparkPlugin = {
    sparkPlugin
  }
}

object RangerSparkPlugin {
  @volatile private var sparkPlugin: RangerSparkPlugin = _
  private val rangerConf: RangerConfiguration = RangerConfiguration.getInstance()
  val sparkSetExcludeKeys: mutable.HashSet[String] = RangerSparkPlugin.build().getOrCreate().sparkSetExcludeKeys
  val showColumnsOption: String = rangerConf.get(
    "xasecure.spark.describetable.showcolumns.authorization.option", "NONE")

  def build(): RangerSparkPlugin = RangerSparkPlugin.synchronized {
    if (sparkPlugin == null) {
      sparkPlugin = new RangerSparkPlugin
      sparkPlugin.init()
    }
    sparkPlugin.checkActive()
    sparkPlugin
  }
}
