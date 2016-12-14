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

import com.sparklinedata.mdformat.MDFormatOptions
import com.sparklinedata.metadata.IndexFieldInfo
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{AnalysisException, SPLMDFUtils, SparkSession}
import org.sparklinedata.druid.metadata.{StarSchema, StarSchemaInfo}

object OLAPFormatUtils {

  val SPARKLINE_PREFIX = StarSchemaInfo.SPARKLINE_PREFIX
  val SPARKLINE_OLAPINDEX_PREFIX = SPARKLINE_PREFIX + ".olapindex."

  def formatOptions(sparkSession : SparkSession,
                    indexName : String,
                    tableName : String,
                    fieldInfos : Seq[IndexFieldInfo],
                    dimensions : Option[String],
                    metrics : Option[String],
                    options : Option[Map[String,String]]
                   ) : MDFormatOptions = {
    var m = options.getOrElse(Map())

    m += (MDFormatOptions.INDEX_NAME -> indexName)
    m += (MDFormatOptions.INDEX_FIELD_INFOS -> SPLMDFUtils.asJson(fieldInfos))

    if (dimensions.isDefined) {
      m += (MDFormatOptions.DIMENSIONS -> dimensions.get)
    }

    if (metrics.isDefined) {
      m += (MDFormatOptions.METRICS -> metrics.get)
    }

    new MDFormatOptions(m, sparkSession)
  }

  def indexSparkSchema(idxOptions : MDFormatOptions,
                       partitionColumns : Seq[String],
                       srcTableSchema : StructType) : StructType = {

    val srcFieldNames = srcTableSchema.fields.map(_.name).toSet
    val idxFields =
      idxOptions.indexFieldInfos.map(_.name) ++
        idxOptions.dimensions ++
        idxOptions.metrics

    (idxFields ++ partitionColumns).foreach { c =>

      if (!srcFieldNames.contains(c) ) {
        throw new AnalysisException(s"Unknown source column '$c' specified in olap index")
      }
    }

    /*
     * put the partition columns as the last columns in the schema.
     */
    val sparkFields =
      srcTableSchema.fields.filter(f => idxFields.contains(f.name)) ++
        srcTableSchema.fields.filter(f => partitionColumns.contains(f.name))

    StructType(sparkFields)
  }

  def olapIndexes(properties : Map[String, String]) : Seq[(String,Boolean)] = {
    properties.filterKeys {
      case k if k.startsWith(SPARKLINE_OLAPINDEX_PREFIX) => true
      case _ => false
    }.map {
      case (k,v) => {
        val idxName = k.substring(SPARKLINE_OLAPINDEX_PREFIX.length)
        (idxName, v.toBoolean)
      }
    }
  }.toSeq

  def olapIndexMetadataPropertyMap(idxName : String, isFull : Boolean) =
    Map(
      SPARKLINE_OLAPINDEX_PREFIX + idxName -> isFull.toString
    )

}
