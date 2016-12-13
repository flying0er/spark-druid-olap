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

import java.io.File
import java.nio.file.Files

import org.apache.spark.sql.hive.test.sparklinedata.TestHive._

abstract class OLAPBaseTest extends StarSchemaBaseTest {

  val tpchPartFolder =
    "src/test/resources/csv/tpch/datascale1/orderLineItemPartSupplierCustomer.part"

  override def beforeAll() = {
    super.beforeAll()

    if (!Files.exists(new File(tpchPartFolder).toPath)) {
      Files.createDirectories(new File(tpchPartFolder).toPath)
      val cT =
        s"""CREATE TABLE if not exists tpch_flat_small_part(o_orderkey integer,
             o_custkey integer,
      o_orderstatus string, o_totalprice double, o_orderdate string, o_orderpriority string,
      o_clerk string,
      o_shippriority integer, o_comment string, l_partkey integer, l_suppkey integer,
      l_linenumber integer,
      l_quantity double, l_extendedprice double, l_discount double, l_tax double,
      l_returnflag string,
      l_linestatus string, l_shipdate string, l_commitdate string, l_receiptdate string,
      l_shipinstruct string,
      l_shipmode string, l_comment string, order_year string, ps_partkey integer,
      ps_suppkey integer,
      ps_availqty integer, ps_supplycost double, ps_comment string,
      s_name string, s_address string,
      s_phone string, s_acctbal double, s_comment string, s_nation string,
      s_region string, p_name string,
      p_mfgr string, p_brand string, p_type string, p_size integer, p_container string,
      p_retailprice double,
      p_comment string, c_name string , c_address string , c_phone string , c_acctbal double,
      c_mktsegment string , c_comment string , c_nation string , c_region string,
      shipYear integer, shipMonth integer)
      USING csv
      OPTIONS (path "$tpchPartFolder",
      header "false", delimiter "|")
              partitioned by (shipYear, shipMonth)""".stripMargin

      sql(cT)

      sql(
        """
          |insert overwrite table tpch_flat_small_part
          |select o.*, year(o.l_shipdate), month(o.l_shipdate)
          |from orderLineItemPartSupplierBase o
        """.stripMargin).show()
    }
  }

}
