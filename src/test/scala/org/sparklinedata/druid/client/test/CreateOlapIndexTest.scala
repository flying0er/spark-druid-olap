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

package org.sparklinedata.druid.client.test

import org.apache.spark.sql.hive.test.sparklinedata.TestHive._

class CreateOlapIndexTest  extends OLAPBaseTest {

  val dimensions = "o_orderkey,o_custkey,o_orderstatus,o_orderpriority,o_clerk," +
    "o_shippriority,o_comment,l_partkey,l_suppkey,l_linenumber,l_returnflag,l_linestatus," +
    "l_shipinstruct,l_shipmode,l_comment,order_year," +
    "ps_partkey,ps_suppkey,s_name,s_address,s_phone,s_comment,s_nation,s_region," +
    "p_mfgr,p_brand,p_type,p_container,p_comment,c_name,c_address,c_phone,c_acctbal," +
    "c_mktsegment,c_comment,c_nation,c_region"
  val metrics = "l_quantity,l_extendedprice,l_discount,l_tax,ps_availqty," +
    "ps_supplycost,s_acctbal,p_size,p_retailprice"

  test("createIndexOnFlat") { td =>
    sql(
      s"""
         |create olap index tpch_flat_index on orderLineItemPartSupplierBase
         |      dimension p_name is not nullable
         |      dimension ps_comment is nullable nullvalue ""
         |      timestamp dimension l_shipdate spark timestampformat "yyyy-MM-dd'T'HH:mm:ss.SSS"
         |                 is index timestamp
         |                 is nullable nullvalue "1992-01-01T00:00:00.000"
         |      timestamp dimension o_orderdate
         |      timestamp dimension l_commitdate
         |          is nullable nullvalue "1992-01-01T00:00:00.000"
         |      timestamp dimension l_receiptdate
         |          is not nullable
         |      metric o_totalprice aggregator doubleSum
         |      dimensions "$dimensions"
         |      metrics "$metrics"
         |      OPTIONS (
         |        path "src/test/resources/spmd/ds_part"
         |      )
      """.stripMargin)
  }

  test("createIndexOnPart") { td =>
    sql(
      s"""
         |create olap index tpch_flat_index on orderLineItemPartSupplierBase
         |      dimensions "$dimensions"
         |      metrics "$metrics"
         |      OPTIONS (
         |        path "src/test/resources/spmd/ds_part"
         |)
         |partition by shipYear, shipMonth
      """.stripMargin)
  }

}
