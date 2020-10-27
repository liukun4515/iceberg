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
import java.util.List;
import org.apache.iceberg.actions.Actions;
import org.apache.iceberg.actions.RemoveOrphanFilesAction;
import org.apache.iceberg.spark.MethodHandleUtil;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.connector.catalog.ProcedureParameter;
import org.apache.spark.sql.connector.catalog.TableCatalog;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

public class RemoveOrphanFilesProcedure extends BaseProcedure {

  // register the invoked method including method name and the type of input parameters
  private static final MethodHandle METHOD_HANDLE = MethodHandleUtil.methodHandle(
      RemoveOrphanFilesProcedure.class,
      "removeOrphanFiles",
      String.class, String.class, Timestamp.class, boolean.class
  );

  /**
   * input parameters:
   * <p>namespace,string,required</p>
   * <p>table,string,required</p>
   * <p>older_than,timestamp,required</p>
   * <p>dry_run,boolean,optional</p>
   */

  private final ProcedureParameter[] inputParameters = new ProcedureParameter[] {
    ProcedureParameter.required("namespace", DataTypes.StringType),
    ProcedureParameter.required("table", DataTypes.StringType),
    ProcedureParameter.required("older_than", DataTypes.TimestampType),
    // default value is false
    ProcedureParameter.optional("dry_run", DataTypes.BooleanType, false)
  };

  private final StructField[] outputParameters = new StructField[] {
      new StructField("table", DataTypes.StringType, false, Metadata.empty()),
      new StructField("older_than", DataTypes.TimestampType, false, Metadata.empty()),
//      new StructField("remove_file_list", DataTypes.createArrayType(DataTypes.StringType, false),
//      false, Metadata.empty()),
      new StructField("dry_run", DataTypes.BooleanType, false, Metadata.empty())
  };

  private final StructType outputType = new StructType(outputParameters);
  private final MethodHandle methodHandle = METHOD_HANDLE.bindTo(this);

  public RemoveOrphanFilesProcedure(TableCatalog catalog) {
    super(catalog);
  }

  public Iterable<Row> removeOrphanFiles(String namespace, String tableName, Timestamp olderThan, boolean dryRun) {
    return modifyIcebergTable(namespace, tableName, icebergTable -> {
      RemoveOrphanFilesAction action = Actions.forTable(icebergTable).removeOrphanFiles();
      if (dryRun) {
        // do nothing for delete function
        action.deleteWith(s -> { });
      }
      List<String> result = action.olderThan(olderThan.getTime())
          .execute();

      Row outputRow = RowFactory.create(tableName, olderThan, dryRun);
      return Collections.singletonList(outputRow);
    });
  }

  @Override
  public ProcedureParameter[] parameters() {
    return inputParameters;
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
