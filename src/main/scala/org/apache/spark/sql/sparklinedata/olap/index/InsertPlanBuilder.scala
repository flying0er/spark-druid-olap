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

import com.sparklinedata.mdformat.MDFormatOptions
import com.sparklinedata.metadata._
import org.apache.spark.sql.catalyst.analysis.UnresolvedAttribute
import org.apache.spark.sql.catalyst.expressions.aggregate.{HyperLogLogPlusPlus, Max, Min, Sum}
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.execution.datasources.{HadoopFsRelation, LogicalRelation}
import org.apache.spark.sql.{AnalysisException, SPLMDFLogging, SparkSession}
import org.sparklinedata.druid.Utils

class InsertPlanBuilder(val indexName: String,
                        val sourceTableName: String,
                        val overwrite: Boolean,
                        val partitionValues: Map[String, String])(
                         implicit val sparkSession: SparkSession)
  extends SPLMDFLogging with StarSchemaPlanBuilder {

  import OLAPFormatUtils._

  val threshold = sparkSession.sessionState.conf.schemaStringLengthThreshold
  val catalog = sparkSession.sessionState.catalog
  val sourceTableId = sparkSession.sessionState.sqlParser.parseTableIdentifier(sourceTableName)
  val sourceTable = catalog.lookupRelation(sourceTableId)
  val sourceTableMetaData = catalog.getTableMetadata(sourceTableId)
  val sourceTableProperties = sourceTableMetaData.properties
  val isFullIndex = {
    val sourceTableOlapIndexes = olapIndexes(sourceTableMetaData.properties)
    sourceTableOlapIndexes.filter {
      case (n, isFull) if n == indexName => true
      case _ => false
    }.headOption.map(_._2).getOrElse(
      throw new AnalysisException(s"$indexName is not an olap index of $sourceTableName, " +
        s"registered olap indexes are: ${sourceTableOlapIndexes.map(_._1).mkString}")
    )
  }

  val indexTableId = sparkSession.sessionState.sqlParser.parseTableIdentifier(indexName)
  val indexTable = catalog.lookupRelation(indexTableId)
  val (indexPartSchema, indexSparkSchema, parameters) = indexDetails(indexTable)
  val options = new MDFormatOptions(parameters, sparkSession)
  val starSchema = getStarSchema(sourceTableName, sourceTableId, catalog)
  val indexSchema =
    new IndexSchema(IndexStorageInfo(options, indexSparkSchema), indexSparkSchema)

  val sizePerTask: Long = {
    (options.preferredSegmentSize * options.indexSizeReductionPercent).toLong
  }

  def inputHasTimestampColumn = indexSchema.timestampColumn.isDefined

  def isPartitionInsert = !partitionValues.isEmpty

  /*
 * 1. Build SelectPlan for StarSchema
 * 2. Add partition predicates
 * 3. If !fullIndex
 *      project dims + metrics + partCols
 *      group by dims + partCols, add metric aggs
 * 4. Cases:
 *     (has TS, has Part Cols) -> distribute by Part Cols, Sort by TS Col
 *     (no TS, has Part Cols) -> distribute by Part Cols
 *     (has TS, no Part Cols) -> collate and sort by TS Col
 *     (no TS, no Part Cols) -> do nothing.
 * 5. Add Insert (overwrite) on top of Step 4.
 * 6. run insert.
 */


  def buildInsertPlan: LogicalPlan = {
    val starJoinPlan = joinPlan
    val withPartitionPredicatesPlan = filterPartitions(starJoinPlan)
    val gPlan = grainPlan(withPartitionPredicatesPlan)
    val rPlan = redistributePlan(gPlan)
    rPlan
  }

  def redistributePlan(plan: LogicalPlan): LogicalPlan = {
    val (hasTS, isPartitionInput) = (inputHasTimestampColumn, isPartitionInsert)
    val inSize = inputSize(sourceTable)
    var currPlan = plan
    val numInTasks = inSize.map(sz => Math.max((sz / sizePerTask).toInt, 1))

    currPlan = if (isPartitionInput) {

      RepartitionByExpression(partitionAttributes, currPlan, numInTasks)
    } else {
      def numPartitions: Int = numInTasks.getOrElse(
        sparkSession.sqlContext.conf.numShufflePartitions
      )
      Repartition(numPartitions, true, currPlan)
    }

    currPlan = if (hasTS) {
      val tsCol = indexSchema.timestampColumn.get
      SortPartitions(
        Seq(SortOrder(
          UnresolvedAttribute(tsCol.sparkColumn),
          Ascending
        )), currPlan)
    } else currPlan

    currPlan
  }

  def inputSize(plan: LogicalPlan): Option[Long] = plan match {
    case LogicalRelation(
    files@HadoopFsRelation(_, partitionSchema, dataSchema, _, _, options),
    _, _) => {
      val sz = if (partitionValues.isEmpty) {
        files.location.allFiles().map(_.getLen).sum
      } else {
        val selectedPartitions = files.location.listFiles(partitionFilters.toList)
        selectedPartitions.flatMap(_.files.map(_.getLen)).sum
      }
      Some(sz)
    }
    case SubqueryAlias(_, p@LogicalRelation(
    HadoopFsRelation(_, partitionSchema, dataSchema, _, _, options),
    _, _)) => inputSize(p)
    case _ => None
  }

  private def enforceSourceAndIndexMatchPartitionScheme: Unit = {
    val srcPartitionColumns = partitionColumns(sourceTableProperties)
    val idxPartitionColumns = indexPartSchema.fieldNames.toSeq
    if (srcPartitionColumns != idxPartitionColumns) {
      throw new AnalysisException(
        s"Partition Inserts on OLAP Index $indexName only allowed " +
          s"when source table and index have the same partition scheme"
      )
    }
  }

  private def grainPlan(plan: LogicalPlan): LogicalPlan = {
    if (isFullIndex) {
      plan
    } else {

      val groupingExpressions: Seq[Expression] = indexSchema.dimensions.map { d =>
        UnresolvedAttribute(d.sparkColumn)
      } ++ {
        indexPartSchema.fields.map { f =>
          UnresolvedAttribute(f.name)
        }
      }

      val aggregateExpressions: Seq[NamedExpression] = {
        indexSchema.metrics.map { m =>
          val ar = UnresolvedAttribute(m.sparkColumn)
          m.aggregator match {
            case DoubleMinAggregator => Min(ar).toAggregateExpression()
            case DoubleMaxAggregator => Max(ar).toAggregateExpression()
            case DoubleSumAggregator => Sum(ar).toAggregateExpression()
            case LongMinAggregator => Min(ar).toAggregateExpression()
            case LongMaxAggregator => Max(ar).toAggregateExpression()
            case LongSumAggregator => Sum(ar).toAggregateExpression()
            case CardinalityAggregator(List(f), true)
              if f == m.name => HyperLogLogPlusPlus(ar).toAggregateExpression()
            case _ => throw new AnalysisException(
              s"Cannot index metric at a higher grain$m : \n\t" +
                "unsupported Aggregation Type for indexing, " +
                "only full index supported this aggregation type.")
          }
        }.map(e => Alias(e, e.toString)())
      }

      Aggregate(
        groupingExpressions,
        aggregateExpressions,
        plan)
    }
  }

  private def partitionAttributes: List[Expression] =
    partitionValues.map {
      case (p, v) => {
        val colIdx = sourceTable.schema.fieldIndex(p)
        UnresolvedAttribute(p)
      }
    }.toList

  private def partitionFilters: Iterable[Expression] = {
    import Utils._
    import org.json4s.JsonDSL._
    import org.json4s.jackson.JsonMethods._

    partitionValues.map {
      case (p, v) => {
        val colIdx = sourceTable.schema.fieldIndex(p)
        val dT = sourceTable.schema(colIdx).dataType
        val ar = sparkSession.sessionState.analyzer.resolveExpression(
          UnresolvedAttribute(p),
          sourceTable)
        val json = render(
          ("dataType" -> dT.jsonValue) ~
            ("value" -> v)
        )
        val lit = Literal.fromJSON(json)
        EqualTo(ar, lit).asInstanceOf[Expression]
      }
    }
  }

  private def filterPartitions(plan: LogicalPlan): LogicalPlan = {
    if (partitionValues.isEmpty) {
      plan
    } else {
      enforceSourceAndIndexMatchPartitionScheme
      val preds = partitionFilters

      val filterCond = preds.tail.foldLeft(preds.head) {
        case (l, r) => And(l, r)
      }

      Filter(filterCond, plan)

    }
  }


}
