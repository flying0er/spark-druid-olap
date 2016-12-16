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

package org.apache.spark.sql.sparklinedata.olap.index

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.analysis.UnresolvedAttribute
import org.apache.spark.sql.catalyst.catalog.SessionCatalog
import org.apache.spark.sql.catalyst.expressions.{And, Attribute, EqualTo, Expression}
import org.apache.spark.sql.catalyst.plans.Inner
import org.apache.spark.sql.catalyst.plans.logical.{Join, LogicalPlan}
import org.sparklinedata.druid.metadata.{StarRelation, StarSchema, StarTable}

trait StarSchemaPlanBuilder {

  val sparkSession: SparkSession
  val catalog: SessionCatalog
  val starSchema: StarSchema

  lazy val tablePlanMap: Map[String, LogicalPlan] = {
    starSchema.tableMap.keySet.map { tblNm =>
      val tableId = sparkSession.sessionState.sqlParser.parseTableIdentifier(tblNm)
      (tblNm -> catalog.lookupRelation(tableId, None))
    }.toMap
  }

  private def attrRef(tableName: String,
                      colName: String): Attribute = {
    val schema = tablePlanMap(tableName).schema
    val colIdx = schema.fieldIndex(colName)
    UnresolvedAttribute(colName)
  }

  private def joinCondition(leftTable: String,
                            rightTable: String,
                            joiningKeys: (String, String)): Expression = {
    EqualTo(attrRef(leftTable, joiningKeys._1), attrRef(rightTable, joiningKeys._2))
  }


  private def joinCondition(table: StarTable,
                            starRelation: StarRelation): Expression = {
    assert(table.parent.get == starRelation)

    val parentTable = starRelation.tableName
    val e = joinCondition(table.name, parentTable, starRelation.joiningKeys.head)
    starRelation.joiningKeys.tail.foldLeft(e) {
      case (e, joiningKeys) => And(e, joinCondition(table.name, parentTable, joiningKeys))
    }
  }

  def isBefore(st1: StarTable, st2: StarTable): Boolean = {
    if (st1 == starSchema.factTable) {
      true
    } else if (st2 == starSchema.factTable) {
      false
    } else if (st1.parent.get.tableName != starSchema.factTable.name) {
      isBefore(starSchema.tableMap(st1.parent.get.tableName), st2)
    } else if (st2.parent.get.tableName != starSchema.factTable.name) {
      isBefore(st1, starSchema.tableMap(st2.parent.get.tableName))
    } else {
      st1.name < st2.name
    }
  }

  def starSchemaTablesInJoinOrder =
    starSchema.tableMap.values.toList.sortWith(isBefore)

  def joinPlan: LogicalPlan = {
    val tables = starSchemaTablesInJoinOrder
    val plan = tablePlanMap(tables.head.name)
    tables.tail.foldLeft(plan) {
      case (plan, starTable) =>
        Join(plan,
          tablePlanMap(starTable.name),
          Inner,
          Some(joinCondition(starTable, starTable.parent.get))
        )
    }

  }
}
