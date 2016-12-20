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

import com.sparklinedata.metadata.IndexSchema
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.analysis.UnresolvedAttribute
import org.apache.spark.sql.catalyst.catalog.SessionCatalog
import org.apache.spark.sql.catalyst.expressions.{And, Attribute, EqualTo, Expression}
import org.apache.spark.sql.catalyst.plans.Inner
import org.apache.spark.sql.catalyst.plans.logical.{Join, LogicalPlan}
import org.apache.spark.sql.types.StructType
import org.sparklinedata.druid.metadata.{StarRelation, StarSchema, StarTable}


/**
  *
  * @param starSchemaToFactRatio based on the fields in the StarSchema tables vs. fields in
  *                              the Fact Table. This ratio >= 1
  * @param indexToStarSchemaRatio based on the fields in the Index vs all the fields in the
  *                               Star Schema. This ratio <= 1
  */
case class OLAPIndexSizeRatios(
                              starSchemaToFactRatio : Double,
                              indexToStarSchemaRatio : Double
                              ) {
  /**
    * Given the factTable size estimate, estimate the input to indexing by
    * marking for the Dimension Table Columns and marking down for the
    * columns not in the OLAP index.
    * @param factTableSz
    * @return
    */
  def indexInputEstimate(factTableSz : Long) : Long = {
    (factTableSz * starSchemaToFactRatio * indexToStarSchemaRatio).toLong
  }
}

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

  lazy val joiningColumns : Set[String] = {
    starSchema.tableMap.values.flatMap{
      case StarTable(_, Some(starRelation)) => {
        starRelation.joiningKeys.flatMap(jk => Set(jk._1, jk._2))
      }
      case _ => Set.empty[String]

    }.toSet
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

  def starSchemaTablesInJoinOrder = {
    val ll = scala.collection.mutable.LinkedHashSet[StarTable]()

    def addTable(starTable: StarTable) : Unit = starTable match {
      case st@StarTable(nm, Some(starRelation)) if !ll.contains(st) => {
        addTable(starSchema.tableMap(starRelation.tableName))
        ll.add(st)
      }
      case st@StarTable(nm, None) if !ll.contains(st) => ll.add(st)
      case _ => ()
    }

    starSchema.tableMap.values.foreach(t => addTable(t))
    ll.toList
  }

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

  def sparkSchema : StructType = {
    joinPlan.schema
  }

  def sizeRatios(indexSchema : IndexSchema) : OLAPIndexSizeRatios = {
    val indexFields = indexSchema.columnMap.values.map(_.name).toList

    def schemaSz(schema : StructType) : Int = {
      schema.fields.map{ f =>
        if (indexSchema.columnMap.contains(f.name)) {
          val iF = indexSchema.columnMap(f.name)
          iF.indexColInfo.avgSize
        } else {
          f.dataType.defaultSize
        }
      }.sum
    }

    val starSchemaRowSize = schemaSz(this.sparkSchema)
    val factRowSize = schemaSz(tablePlanMap(starSchema.factTable.name).schema)
    OLAPIndexSizeRatios(
      starSchemaRowSize.toDouble / factRowSize.toDouble,
      indexSchema.estimateInputRowSize.toDouble / starSchemaRowSize.toDouble
    )
  }
}
