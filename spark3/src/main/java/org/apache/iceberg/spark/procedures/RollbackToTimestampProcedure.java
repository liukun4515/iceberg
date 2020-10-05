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
import java.sql.Timestamp;
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

public class RollbackToTimestampProcedure extends BaseProcedure {

  private static final MethodHandle METHOD_HANDLE = MethodHandleUtil.methodHandle(
      RollbackToTimestampProcedure.class,
      "rollbackToTimestamp",
      String.class, String.class, Timestamp.class);

  private final ProcedureParameter[] parameters = new ProcedureParameter[]{
      ProcedureParameter.required("namespace", DataTypes.StringType),
      ProcedureParameter.required("table", DataTypes.StringType),
      ProcedureParameter.required("timestamp", DataTypes.TimestampType)
  };
  private final StructField[] outputFields = new StructField[]{
      new StructField("previous_current_snapshot_id", DataTypes.LongType, false, Metadata.empty()),
      new StructField("current_snapshot_id", DataTypes.LongType, false, Metadata.empty())
  };
  private final StructType outputType = new StructType(outputFields);
  private final MethodHandle methodHandle = METHOD_HANDLE.bindTo(this);

  public RollbackToTimestampProcedure(TableCatalog catalog) {
    super(catalog);
  }

  public Iterable<Row> rollbackToTimestamp(String namespace, String tableName, Timestamp timestamp) {
    return modifyIcebergTable(namespace, tableName, table -> {
      Snapshot previousCurrentSnapshot = table.currentSnapshot();
      Long previousCurrentSnapshotId = previousCurrentSnapshot != null ? previousCurrentSnapshot.snapshotId() : null;

      table.manageSnapshots()
          .rollbackToTime(timestamp.getTime())
          .commit();

      // TODO: concurrent modifications on the table
      Row outputRow = RowFactory.create(previousCurrentSnapshotId, table.currentSnapshot().snapshotId());
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
