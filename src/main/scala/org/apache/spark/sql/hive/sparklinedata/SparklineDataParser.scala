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

package org.apache.spark.sql.hive.sparklinedata

import com.sparklinedata.metadata._
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.catalyst.parser.{ParseException, ParserInterface}
import org.apache.spark.sql.{SQLContext, SparkSession}
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.catalyst.rules.RuleExecutor
import org.apache.spark.sql.sparklinedata.commands._
import org.apache.spark.sql.sparklinedata.olap.index.OLAPFormatUtils
import org.apache.spark.sql.util.PlanUtil
import org.sparklinedata.druid.metadata.{EqualityCondition, FunctionalDependencyType, StarRelationInfo, StarSchemaInfo}

class SPLParser(sparkSession: SparkSession,
                baseParser : ParserInterface,
                moduleParserExtensions : Seq[SparklineDataParser] = Nil,
                moduleParserTransforms : Seq[RuleExecutor[LogicalPlan]] = Nil
                          ) extends ParserInterface {
  val parsers = {
    if (moduleParserExtensions.isEmpty) {
      Seq(new SparklineDruidCommandsParser(sparkSession))
    } else {
      moduleParserExtensions
    }
  }

  override def parsePlan(sqlText: String): LogicalPlan = {

    val r : SparklineDataParser#ParseResult[LogicalPlan] = null

    val splParsedPlan = parsers.foldRight(r) {
      case (p, o) if o == null || !o.successful => p.parse2(sqlText)
      case (_, o)  => o
    }

    val parsedPlan = if (splParsedPlan.successful ) {
      splParsedPlan.get
    } else {
      try {
        baseParser.parsePlan(sqlText)
      } catch {
        case pe : ParseException => {
          val splFailureDetails = splParsedPlan.asInstanceOf[SparklineDataParser#NoSuccess].msg
          throw new ParseException(pe.command,
            pe.message + s"\nSPL parse attempt message: $splFailureDetails",
            pe.start,
            pe.stop
          )
        }
      }
    }

    moduleParserTransforms.foldRight(parsedPlan){
      case (rE, lP) => rE.execute(lP)
    }
  }

  def parseExpression(sqlText: String): Expression =
    baseParser.parseExpression(sqlText)

  def parseTableIdentifier(sqlText: String): TableIdentifier =
  baseParser.parseTableIdentifier(sqlText)
}

abstract class SparklineDataParser extends AbstractSparkSQLParser {

  def parse2(input: String): ParseResult[LogicalPlan]
}

class SparklineDruidCommandsParser(sparkSession: SparkSession) extends SparklineDataParser {

  protected val CLEAR = Keyword("CLEAR")
  protected val DRUID = Keyword("DRUID")
  protected val CACHE = Keyword("CACHE")
  protected val DRUIDDATASOURCE = Keyword("DRUIDDATASOURCE")
  protected val ON = Keyword("ON")
  protected val EXECUTE = Keyword("EXECUTE")
  protected val QUERY = Keyword("QUERY")
  protected val USING = Keyword("USING")
  protected val HISTORICAL = Keyword("HISTORICAL")
  protected val EXPLAIN = Keyword("EXPLAIN")
  protected val REWRITE = Keyword("REWRITE")
  protected val ONE_TO_ONE = Keyword("ONE_TO_ONE")
  protected val MANY_TO_ONE = Keyword("MANY_TO_ONE")
  protected val JOIN = Keyword("JOIN")
  protected val OF = Keyword("OF")
  protected val WITH = Keyword("WITH")
  protected val AND = Keyword("AND")
  protected val CREATE = Keyword("CREATE")
  protected val ALTER = Keyword("ALTER")
  protected val STAR = Keyword("STAR")
  protected val SCHEMA = Keyword("SCHEMA")
  protected val AS = Keyword("AS")
  protected val OLAP = Keyword("OLAP")
  protected val INDEX = Keyword("INDEX")
  protected val DIMENSION = Keyword("DIMENSION")
  protected val TIMESTAMP = Keyword("TIMESTAMP")
  protected val IS = Keyword("IS")
  protected val NOT = Keyword("NOT")
  protected val NULLABLE = Keyword("NULLABLE")
  protected val NULLVALUE = Keyword("NULLVALUE")
  protected val SPARK = Keyword("SPARK")
  protected val TIMESTAMPFORMAT = Keyword("TIMESTAMPFORMAT")
  protected val METRIC = Keyword("METRIC")
  protected val AGGREGATOR = Keyword("AGGREGATOR")
  protected val DOUBLEMAX = Keyword("doublemax")
  protected val LONGMAX = Keyword("longmax")
  protected val DOUBLEMIN = Keyword("doublemin")
  protected val LONGMIN = Keyword("longmin")
  protected val COUNT = Keyword("count")
  protected val DOUBLESUM = Keyword("doublesum")
  protected val LONGSUM = Keyword("longsum")
  protected val HYPERUNIQUE = Keyword("hyperunique")
  protected val JAVASCRIPT = Keyword("JAVASCRIPT")
  protected val FUNCTIONS = Keyword("FUNCTIONS")
  protected val AGGREGATE = Keyword("AGGREGATE")
  protected val COMBINE = Keyword("COMBINE")
  protected val RESET = Keyword("RESET")
  protected val DIMENSIONS = Keyword("DIMENSIONS")
  protected val METRICS = Keyword("METRICS")
  protected val OPTIONS = Keyword("OPTIONS")
  protected val BY = Keyword("BY")
  protected val PARTITION = Keyword("PARTITION")
  protected val INSERT = Keyword("INSERT")
  protected val OVERWRITE = Keyword("OVERWRITE")
  protected val PARTITIONS = Keyword("PARTITIONS")


  def parse2(input: String): ParseResult[LogicalPlan] = synchronized {
    // Initialize the Keywords.
    initLexical
    phrase(start)(new lexical.Scanner(input))
  }

  protected override lazy val start: Parser[LogicalPlan] =
    clearDruidCache | execDruidQuery | explainDruidRewrite |
      starSchema | createOlapIndex | insertOlapIndex

  protected lazy val clearDruidCache: Parser[LogicalPlan] =
    CLEAR ~> DRUID ~> CACHE ~> opt(ident) ^^ {
      case id => ClearMetadata(id)
    }

  protected lazy val execDruidQuery : Parser[LogicalPlan] =
    (ON ~> DRUIDDATASOURCE ~> ident) ~ (USING ~> HISTORICAL).? ~
      (EXECUTE ~> opt(QUERY) ~> restInput) ^^ {
      case ds ~ hs ~ query => {
        PlanUtil.logicalPlan(ds, query, hs.isDefined)(sparkSession.sqlContext)
      }
    }

  protected lazy val explainDruidRewrite: Parser[LogicalPlan] =
    EXPLAIN ~> DRUID ~> REWRITE ~> restInput ^^ {
      case sql => ExplainDruidRewrite(sql)
    }

  private lazy val starRelationType : Parser[FunctionalDependencyType.Value] =
    (ONE_TO_ONE | MANY_TO_ONE) ^^ {
      case s if s == ONE_TO_ONE.normalize => FunctionalDependencyType.OneToOne
      case s if s == MANY_TO_ONE.normalize => FunctionalDependencyType.ManyToOne
    }

  private lazy val joinEqualityCond : Parser[EqualityCondition] =
    (qualifiedId <~ "=") ~ qualifiedId ^^ {
      case ~(l,r) => EqualityCondition(l,r)
    }

  private lazy val starRelation : Parser[StarRelationInfo] =
    starRelationType.? ~ JOIN ~ OF ~ qualifiedId ~
      WITH ~ qualifiedId ~ ON ~ repsep(joinEqualityCond, AND) ^^ {
      case srI ~ _ ~ _ ~ leftTable ~ _ ~ rightTable ~ _ ~ joinEqConds => {
        StarRelationInfo(
          leftTable,
          rightTable,
          srI.getOrElse(FunctionalDependencyType.ManyToOne),
          joinEqConds
        )
      }
    }

  private lazy val starSchema : Parser[LogicalPlan] =
    createOrAlter ~ STAR ~ SCHEMA ~ ON ~ qualifiedId ~ opt(AS ~ repsep(starRelation, opt(","))) ^^ {
      case c ~ _ ~ _ ~ _ ~ factTable ~ Some(_ ~ starRelations)  =>
        CreateStarSchema(StarSchemaInfo(factTable, starRelations:_*), !c)
      case c ~ _ ~ _ ~ _ ~ factTable ~ None  => CreateStarSchema(StarSchemaInfo(factTable), !c)
    }

  private lazy val indexDimensionInfo : Parser[IndexDimensionInfo] = {
    DIMENSION ~> ident ~ (
      (IS ~ NULLABLE ~ NULLVALUE ~ stringLit) |
        opt((IS ~ NOT ~ NULLABLE))
      ) ^^ {
      case c ~ (_ ~ _ ~ _ ~ s) => IndexDimensionInfo(c, true, s.toString)
      case id ~ _ => IndexDimensionInfo(id, false, null)
    }
  }

  private lazy val indexTimestampDimensionInfo : Parser[IndexTimestampDimensionInfo] = {
    TIMESTAMP ~> DIMENSION ~> ident ~
      opt(SPARK ~> TIMESTAMPFORMAT ~> stringLit) ~
      opt(IS ~> INDEX ~> TIMESTAMP) ~ (
      (IS ~ NULLABLE ~ NULLVALUE ~ stringLit) |
        opt((IS ~ NOT ~ NULLABLE))
      ) ^^ {
      case c ~ tsFmt ~ isIdx ~ (_ ~ _ ~ _ ~ s) =>
        IndexTimestampDimensionInfo(c, true, s.toString, tsFmt, isIdx.isDefined)
      case c ~ tsFmt ~ isIdx ~ _ =>
        IndexTimestampDimensionInfo(c, false, null, tsFmt, isIdx.isDefined)
    }
  }
  
  private lazy val metricBasicAggergator : Parser[IndexAggregator] = {
    AGGREGATOR ~>
      (DOUBLEMAX | LONGMAX | DOUBLEMIN | LONGMIN | COUNT | DOUBLESUM | LONGSUM | HYPERUNIQUE) ^^ {
      case DOUBLEMAX.str => DoubleMaxAggregator
      case LONGMAX.str => LongMaxAggregator
      case DOUBLEMIN.str => DoubleMinAggregator
      case LONGMIN.str => LongMinAggregator
      case COUNT.str => CountAggregator
      case DOUBLESUM.str => DoubleSumAggregator
      case LONGSUM.str => LongSumAggregator
      case HYPERUNIQUE.str => HyperUniquesAggregator
    }
  }

  private lazy val javaScriptAggergator : Parser[IndexAggregator] = {
    (AGGREGATOR ~ JAVASCRIPT) ~> rep1sep(ident, ",") ~
      ( (FUNCTIONS ~> AGGREGATE ~> stringLit) ~
        (COMBINE ~> stringLit) ~
        (RESET ~> stringLit)
        ) ^^ {
      case fieldNames ~ (fnAggregate ~ fnReset ~ fnCombine)  =>
        JavaScriptAggregator(fieldNames, fnAggregate, fnReset, fnCombine)
    }
  }

  private lazy val aggregator =
    metricBasicAggergator | javaScriptAggergator

  private lazy val indexMetricInfo : Parser[IndexMetricInfo] = {
    METRIC ~> ident ~ aggregator ~ (
      (IS ~ NULLABLE ~ NULLVALUE ~ stringLit) |
        opt((IS ~ NOT ~ NULLABLE))
      ) ^^ {
      case c ~ agg ~ (_ ~ _ ~ _ ~ s) => IndexMetricInfo(c, true, s.toString, agg)
      case id ~ agg ~ _ => IndexMetricInfo(id, false, null, agg)
    }
  }

  private lazy val fieldInfo : Parser[IndexFieldInfo] =
    (indexDimensionInfo | indexTimestampDimensionInfo | indexMetricInfo)

  private lazy val fieldInfos : Parser[Seq[IndexFieldInfo]] =
    rep(fieldInfo)

  private lazy val option : Parser[(String,String)] =
    ident ~ stringLit ^^ {
      case k ~ v => (k,v)
    }

  private lazy val partitionBy : Parser[Seq[String]] =
    (PARTITION ~ BY) ~> rep1sep(ident, ",")

  private lazy val options : Parser[Map[String,String]] =
    rep1sep(option, opt(",")).map(_.toMap)

  private lazy val createOlapIndex : Parser[LogicalPlan] =
  (CREATE ~ OLAP ~ INDEX) ~> qualifiedId ~ (ON ~> qualifiedId) ~
    fieldInfos ~
    opt((DIMENSIONS ~> stringLit)) ~
    opt((METRICS ~> stringLit)) ~
    opt((OPTIONS ~ "(") ~> options <~ ")") ~
    opt(partitionBy) ^^ {
      case indexName ~ tableName ~ fieldInfos ~ dimensions ~ metrics ~ options  ~ partitionBy => {
        val mdFmtOptions = OLAPFormatUtils.formatOptions(
          sparkSession, indexName, tableName, fieldInfos, dimensions, metrics, options
        )
        CreateOlapIndex(indexName, tableName, mdFmtOptions, partitionBy.getOrElse(Seq()))
      }
    }

  private lazy val partPredicate : Parser[(String,String)] =
    ident ~ "=" ~ stringLit ^^ {
      case k ~ _ ~ v => (k,v)
    }

  private lazy val partitionsPredicates : Parser[Map[String,String]] =
    rep1sep(partPredicate, opt(",")).map(_.toMap)


  private lazy val insertOlapIndex : Parser[LogicalPlan] =
    (INSERT ~> opt(OVERWRITE) <~ OLAP <~ INDEX) ~ qualifiedId ~ (OF ~> qualifiedId) ~
      opt(PARTITIONS ~> partitionsPredicates) ^^ {
      case ovwrt ~ indexName ~ tableName ~ partPreds =>
        InsertOlapIndex(indexName, tableName, ovwrt.isDefined, partPreds.getOrElse(Map()))
    }

  private lazy val createOrAlter : Parser[Boolean] =
    (CREATE | ALTER ) ^^ {
      case s if s == CREATE.normalize => true
      case _ => false
    }

  private lazy val qualifiedId : Parser[String] =
    (ident ~ ("." ~> ident).?) ^^ {
      case ~(n, None) => n
      case ~(q, Some(n)) => s"$q.$n"
    }

}