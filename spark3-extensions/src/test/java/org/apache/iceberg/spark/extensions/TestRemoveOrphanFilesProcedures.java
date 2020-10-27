/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.iceberg.spark.extensions;

import java.sql.Timestamp;
import java.time.LocalDateTime;
import java.util.List;
import java.util.Map;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.Table;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.junit.After;
import org.junit.Assert;
import org.junit.Test;

public class TestRemoveOrphanFilesProcedures extends SparkExtensionsTestBase{

  public TestRemoveOrphanFilesProcedures(
      String catalogName,
      String implementation,
      Map<String, String> config) {
    super(catalogName, implementation, config);
  }

  @After
  public void tearDown() throws Exception {
    sql("DROP TABLE IF EXISTS %s", tableName);
  }

  @Test
  public void testRemoveOrphanFilesWithDryrun() {
    sql("CREATE TABLE %s (id bigint NOT NULL, data string) USING iceberg", tableName);
    sql("INSERT INTO TABLE %s VALUES (1, 'a')", tableName);

    Table table = validationCatalog.loadTable(tableIdent);
    Snapshot firstSnapshot = table.currentSnapshot();

    sql("INSERT INTO TABLE %s VALUES (1, 'a')", tableName);

    assertEquals("Should have expected rows",
            ImmutableList.of(row(1L, "a"), row(1L, "a")),
            sql("SELECT * FROM %s ORDER BY id", tableName));


    LocalDateTime localDateTime = LocalDateTime.now();
    Timestamp timestamp = Timestamp.valueOf(localDateTime);

    List<Object[]> output = sql(
            "CALL %s.system.remove_orphan_files" +
                    "(namespace => '%s',table => '%s', " +
                    "older_than => TIMESTAMP '%s', dry_run => %s)",
            catalogName, tableIdent.namespace(), tableIdent.name(), timestamp.toString(), Boolean.FALSE);
    Assert.assertEquals(1, output.size());
    Assert.assertEquals(3, output.get(0).length);
    Assert.assertEquals(String.valueOf(output.get(0)[0]), tableIdent.name());
  }

  @Test
  public void testRemoveOrphanFilesWithouthDryrun() {

  }
}
