/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.sql.sparklinedata.commands

import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.execution.command.{AlterTableSetPropertiesCommand, RunnableCommand}
import org.apache.spark.sql.sources.druid.{DruidPlanner, DruidQueryCostModel}
import org.apache.spark.sql.types._
import org.apache.spark.sql.{AnalysisException, Row, SparkSession}
import org.sparklinedata.druid.metadata._

import com.sparklinedata.mdformat.MDFormatOptions

case class ClearMetadata(druidHost: Option[String]) extends RunnableCommand {

  override val output: Seq[Attribute] = {
    val schema = StructType(
      StructField("", StringType, nullable = true) :: Nil)

    schema.toAttributes
  }

  override def run(sparkSession: SparkSession): Seq[Row] = {
    if (druidHost.isDefined) {
      DruidMetadataCache.clearCache(druidHost.get)
    } else {
      DruidMetadataCache.clearCache
    }
    Seq(Row(""))
  }
}

case class ExplainDruidRewrite(sql: String) extends RunnableCommand {

  override val output: Seq[Attribute] = {
    val schema = StructType(
      StructField("", StringType, nullable = true) :: Nil)

    schema.toAttributes
  }

  override def run(sparkSession: SparkSession): Seq[Row] = {
    val qe = sparkSession.sessionState.executeSql(sql)

    qe.sparkPlan.toString().split("\n").map(Row(_)).toSeq ++
    Seq(Row("")) ++
    DruidPlanner.getDruidRDDs(qe.sparkPlan).flatMap { dR =>
      val druidDSIntervals  = dR.drDSIntervals
      val druidDSFullName= dR.drFullName
      val druidDSOptions = dR.drOptions
      val inputEstimate = dR.inputEstimate
      val outputEstimate = dR.outputEstimate

      s"""DruidQuery(${System.identityHashCode(dR.dQuery)}) details ::
         |${DruidQueryCostModel.computeMethod(
        sparkSession.sqlContext, druidDSIntervals, druidDSFullName, druidDSOptions,
        inputEstimate, outputEstimate, dR.dQuery.q)
      }
       """.stripMargin.split("\n").map(Row(_))
    }
  }
}

case class CreateStarSchema(starSchemaInfo : StarSchemaInfo,
                            update : Boolean) extends RunnableCommand with Logging {

  override val output: Seq[Attribute] = {
    val schema = StructType(
      StructField("", StringType, nullable = true) :: Nil)

    schema.toAttributes
  }

  override def run(sparkSession: SparkSession): Seq[Row] = {

    val threshold = sparkSession.sessionState.conf.schemaStringLengthThreshold
    val catalog = sparkSession.sessionState.catalog

    /*
     * qualify TableNames
     */
    val qInfo = StarSchemaInfo.qualifyTableNames(sparkSession.sqlContext, starSchemaInfo)
    val tabId = sparkSession.sessionState.sqlParser.parseTableIdentifier(qInfo.factTable)

    /*
     * validate the Star Schema
     */
    StarSchema(qInfo.factTable, qInfo, true)(sparkSession.sqlContext) match {
      case Left(errMsg) => throw new AnalysisException(errMsg)
      case _ => ()
    }

    val existingTableProperties = catalog.getTableMetadata(tabId).properties
    val existingStarSchema = StarSchemaInfo.fromMetadataMap(existingTableProperties)

    if ( !update && existingStarSchema.isDefined ) {
      throw new AnalysisException(
        s"Cannot create starSchema on $tabId, there is already a Star Schema on it, " +
          s"call alter star schema")
    }

    val starSchemaProps = StarSchemaInfo.toMetadataMap(starSchemaInfo, threshold)
    val alterTableCmd = new AlterTableSetPropertiesCommand(
      tabId,
      starSchemaProps,
      false)

    log.info(s"Setting Star Schema for table ${starSchemaInfo.factTable}: \n {}",
      StarSchemaInfo.toJsonString(qInfo))

    alterTableCmd.run(sparkSession)

  }
}

case class CreateOlapIndex(indexName : String,
                           sourceTableName : String,
                           options : MDFormatOptions,
                           partitionColumns : Seq[String]) extends RunnableCommand with Logging {

  override def run(sparkSession: SparkSession): Seq[Row] = {

    val threshold = sparkSession.sessionState.conf.schemaStringLengthThreshold
    val catalog = sparkSession.sessionState.catalog

    val sourceTableId = sparkSession.sessionState.sqlParser.parseTableIdentifier(sourceTableName)

    val sourceTable = catalog.lookupRelation(sourceTableId)
    val sourceTableMetaData = catalog.getTableMetadata(sourceTableId)

    sourceTable.schema

    /*
     * 1. Find the sourceTableName
     * 2. Find or Create the StarSchema on this table.
     * 3. Create Index Schema from MDFormatOptions + SourceTable
     * 4. Build the IndexStorageInfo from Index Schema + MDFormatOptions
     * 5. Build Select LogicalPlan from StarSchema
     * 6. If all columns from Source are not included, add Distinct Operator on top of 5.
     * 7.
     */

    Seq()
  }
}
