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
import org.apache.spark.sql.{SPLMDFUtils, SparkSession}

object OLAPFormatUtils {

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

}
