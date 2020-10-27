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

package org.apache.iceberg.spark.procedures;

import java.lang.invoke.MethodHandle;
import java.util.Collections;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.spark.MethodHandleUtil;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.connector.catalog.ProcedureParameter;
import org.apache.spark.sql.connector.catalog.TableCatalog;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

public class CherrypickSnapshotProcedure extends BaseProcedure {

  private static final MethodHandle METHOD_HANDLE = MethodHandleUtil.methodHandle(
      CherrypickSnapshotProcedure.class,
      "cherrypickSnapshot",
      String.class, String.class, long.class);

  private final ProcedureParameter[] parameters = new ProcedureParameter[]{
      ProcedureParameter.required("namespace", DataTypes.StringType),
      ProcedureParameter.required("table", DataTypes.StringType),
      ProcedureParameter.required("snapshot_id", DataTypes.LongType)
  };
  private final StructField[] outputFields = new StructField[]{
      new StructField("source_snapshot_id", DataTypes.LongType, false, Metadata.empty()),
      new StructField("current_snapshot_id", DataTypes.LongType, false, Metadata.empty())
  };
  private final StructType outputType = new StructType(outputFields);
  private final MethodHandle methodHandle = METHOD_HANDLE.bindTo(this);

  public CherrypickSnapshotProcedure(TableCatalog catalog) {
    super(catalog);
  }

  public Iterable<Row> cherrypickSnapshot(String namespace, String tableName, long snapshotId) {
    return modifyIcebergTable(namespace, tableName, table -> {
      table.manageSnapshots()
          .cherrypick(snapshotId)
          .commit();

      Snapshot currentSnapshot = table.currentSnapshot();
      long currentSnapshotId = currentSnapshot.snapshotId();

      Row outputRow = RowFactory.create(snapshotId, currentSnapshotId);
      return Collections.singletonList(outputRow);
    });
  }

  @Override
  public ProcedureParameter[] parameters() {
    return parameters;
  }

  @Override
  public StructType outputType() {
    return outputType;
  }

  @Override
  public MethodHandle methodHandle() {
    return methodHandle;
  }
}