/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.iceberg.actions;

import java.util.Map;
import org.apache.iceberg.Table;
import org.apache.iceberg.spark.SparkCatalogTestBase;
import org.junit.After;
import org.junit.Test;

public class TestAction3SparkCatalog extends SparkCatalogTestBase {
  public TestAction3SparkCatalog(String catalogName, String implementation, Map<String, String> config) {
    super(catalogName, implementation, config);
  }

  @After
  public void dropTable() {
    sql("DROP TABLE IF EXISTS %s", tableName);
  }

  @Test
  public void testActions() {
    sql("CREATE TABLE %s (id BIGINT NOT NULL, data STRING) USING iceberg", tableName);
    Table table = validationCatalog.loadTable(tableIdent);
    Actions actions = Actions.forTable(table);
    Action action = actions.removeOrphanFiles();
    action.execute();

    action = actions.expireSnapshots();
    action.execute();

    action = actions.rewriteDataFiles();
    action.execute();

    action = actions.rewriteManifests();
    action.execute();
  }
}
