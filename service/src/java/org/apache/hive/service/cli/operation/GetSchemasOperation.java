/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hive.service.cli.operation;

import java.util.ArrayList;
import java.util.Arrays;

import org.apache.hadoop.hive.metastore.IMetaStoreClient;
import org.apache.hadoop.hive.ql.security.authorization.plugin.HiveOperationType;
import org.apache.hive.service.cli.FetchOrientation;
import org.apache.hive.service.cli.HiveSQLException;
import org.apache.hive.service.cli.OperationState;
import org.apache.hive.service.cli.OperationType;
import org.apache.hive.service.cli.RowSet;
import org.apache.hive.service.cli.RowSetFactory;
import org.apache.hive.service.cli.TableSchema;
import org.apache.hive.service.cli.session.HiveSession;

/**
 * GetSchemasOperation.
 *
 */
public class GetSchemasOperation extends MetadataOperation {
  private final String catalogName;
  private final String schemaName;

  private static final TableSchema RESULT_SET_SCHEMA = new TableSchema()
  .addStringColumn("TABLE_SCHEM", "Schema name.")
  .addStringColumn("TABLE_CATALOG", "Catalog name.");

  private RowSet rowSet;

  // 获取操作类型
  protected GetSchemasOperation(HiveSession parentSession, String catalogName, String schemaName) {
    super(parentSession, OperationType.GET_SCHEMAS);
    this.catalogName = catalogName;
    this.schemaName = schemaName;
    this.rowSet = RowSetFactory.create(RESULT_SET_SCHEMA, getProtocolVersion(), false);
  }

  @Override
  public void runInternal() throws HiveSQLException {
    setState(OperationState.RUNNING);
    if (isAuthV2Enabled()) {
      String cmdStr = "catalog : " + catalogName + ", schemaPattern : " + schemaName;
      authorizeMetaGets(HiveOperationType.GET_SCHEMAS, null, cmdStr);
    }
    try {
      // 获取HSC
      IMetaStoreClient metastoreClient = getParentSession().getMetaStoreClient();
      String schemaPattern = convertSchemaPattern(schemaName);
      // 调用HSC.getDatabases(schemaPattern) 获取到结果放到RowSet 中
      // show databases like '%ss%'
      for (String dbName : metastoreClient.getDatabases(schemaPattern)) {
        rowSet.addRow(new Object[] {dbName, DEFAULT_HIVE_CATALOG});
      }
      setState(OperationState.FINISHED);
    } catch (Exception e) {
      setState(OperationState.ERROR);
      throw new HiveSQLException(e);
    }
  }


  /* (non-Javadoc)
   * @see org.apache.hive.service.cli.Operation#getResultSetSchema()
   */
  @Override
  public TableSchema getResultSetSchema() throws HiveSQLException {
    assertState(new ArrayList<OperationState>(Arrays.asList(OperationState.FINISHED)));
    return RESULT_SET_SCHEMA;
  }

  /* (non-Javadoc)
   * @see org.apache.hive.service.cli.Operation#getNextRowSet(org.apache.hive.service.cli.FetchOrientation, long)
   */
  @Override
  public RowSet getNextRowSet(FetchOrientation orientation, long maxRows) throws HiveSQLException {
    assertState(new ArrayList<OperationState>(Arrays.asList(OperationState.FINISHED)));
    validateDefaultFetchOrientation(orientation);
    if (orientation.equals(FetchOrientation.FETCH_FIRST)) {
      rowSet.setStartOffset(0);
    }
    return rowSet.extractSubset((int)maxRows);
  }
}
