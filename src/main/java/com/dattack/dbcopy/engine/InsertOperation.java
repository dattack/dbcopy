/*
 * Copyright (c) 2017, The Dattack team (http://www.dattack.com)
 *
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
package com.dattack.dbcopy.engine;

import com.dattack.dbcopy.beans.InsertOperationBean;
import com.dattack.jtoolbox.commons.configuration.ConfigurationUtil;
import com.dattack.jtoolbox.jdbc.JDBCUtils;
import com.dattack.jtoolbox.jdbc.JNDIDataSource;
import com.dattack.jtoolbox.jdbc.NamedParameterPreparedStatement;
import org.apache.commons.configuration.AbstractConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.Writer;
import java.sql.*;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.Callable;

/**
 * Executes the INSERT operations.
 *
 * @author cvarela
 * @since 0.1
 */
class InsertOperation implements Callable<Integer> {

    private final static Logger LOGGER = LoggerFactory.getLogger(InsertOperation.class);

    private final InsertOperationBean bean;
    private final DataTransfer dataTransfer;
    private final AbstractConfiguration configuration;
    private Connection connection;
    private NamedParameterPreparedStatement preparedStatement;
    private DbCopyTaskResult taskResult;
    private int rowNumber;
    private volatile List<ColumnMetadata> columnsMetadata2Process;

    public InsertOperation(final InsertOperationBean bean, final DataTransfer dataTransfer,
                           final AbstractConfiguration configuration, DbCopyTaskResult taskResult) {
        this.bean = bean;
        this.dataTransfer = dataTransfer;
        this.configuration = configuration;
        this.taskResult = taskResult;
        this.rowNumber = 0;
    }

    private int addBatch() throws SQLException {
        getPreparedStatement().addBatch();
        rowNumber++;
        int insertedRows = 0;
        if (rowNumber % bean.getBatchSize() == 0) {
            insertedRows = executeBatch();
            LOGGER.debug("Inserted rows: {} (Current block: {})", insertedRows, rowNumber);
        }
        return insertedRows;
    }

    private int executeBatch() throws SQLException {

        int insertedRows = 0;
        try {
            final int[] batchResult = getPreparedStatement().executeBatch();

            for (int result : batchResult) {
                if (result > 0) {
                    insertedRows += result;
                } else if (result == Statement.SUCCESS_NO_INFO) {
                    insertedRows++;
                }
            }

        } catch (final BatchUpdateException e) {
            LOGGER.warn("Batch operation failed: {} (SQLSTATE: {}, Error code: {}, Executed statements: {})",
                    e.getMessage(), e.getSQLState(), e.getErrorCode(), e.getUpdateCounts().length);
        }

        getConnection().commit();
        return insertedRows;
    }

    public int flush() throws SQLException {

        int insertedRows = 0;
        if (rowNumber % bean.getBatchSize() != 0) {
            insertedRows = executeBatch();
        }

        JDBCUtils.closeQuietly(preparedStatement);
        JDBCUtils.closeQuietly(connection);

        return insertedRows;
    }

    private synchronized Connection getConnection() throws SQLException {
        if (connection == null) {
            connection = new JNDIDataSource(ConfigurationUtil.interpolate(bean.getDatasource(), configuration))
                    .getConnection();
            if (bean.getBatchSize() > 0) {
                connection.setAutoCommit(false);
            }
        }
        return connection;
    }

    private NamedParameterPreparedStatement getPreparedStatement() throws SQLException {
        if (preparedStatement == null) {
            preparedStatement = NamedParameterPreparedStatement.build(getConnection(),
                    ConfigurationUtil.interpolate(bean.getSql(), configuration));
        }
        return preparedStatement;
    }

    @Override
    public Integer call() throws SQLException, InterruptedException {

        int totalInsertedRows = 0;
        while (true) {
            List<Object> row = dataTransfer.transfer();
            if (row == null) {
                break;
            }

            populateStatement(getPreparedStatement(), row);

            int insertedRows;
            if (bean.getBatchSize() > 0) {
                insertedRows = addBatch();
            } else {
                insertedRows = getPreparedStatement().executeUpdate();
            }
            taskResult.addProcessedRows(insertedRows);
            totalInsertedRows += insertedRows;
        }

        int insertedRows = flush();
        taskResult.addProcessedRows(insertedRows);

        totalInsertedRows += insertedRows;
        return totalInsertedRows;
    }

    private void populateBlob(NamedParameterPreparedStatement preparedStatement, Blob value, String columnName)
            throws SQLException {

        try {
            Blob targetBlob = preparedStatement.getConnection().createBlob();
            OutputStream output = targetBlob.setBinaryStream(1);

            InputStream in = value.getBinaryStream();
            byte[] buf = new byte[1024];
            int len;
            while ((len = in.read(buf)) != -1) {
                output.write(buf, 0, len);
            }

            preparedStatement.setBlob(columnName, targetBlob);
            value.free();
        } catch (IOException e) {
            throw new SQLException("Unable to create Clob object: " + e.getMessage());
        }
    }

    private void populateClob(NamedParameterPreparedStatement preparedStatement, Clob value, String columnName)
            throws SQLException {

        try {
            Clob targetClob = preparedStatement.getConnection().createClob();
            Writer clobWriter = targetClob.setCharacterStream(1);
            clobWriter.write(value.getSubString(0L, (int) value.length()));
            preparedStatement.setClob(columnName, targetClob);
            value.free();
        } catch (IOException e) {
            throw new SQLException("Unable to create Clob object: " + e.getMessage());
        }
    }

    private void populateSqlXml(NamedParameterPreparedStatement preparedStatement, SQLXML value, String columnName)
            throws SQLException {
        SQLXML targetXml = preparedStatement.getConnection().createSQLXML();
        targetXml.setString(value.getString());
        preparedStatement.setSQLXML(columnName, targetXml);
        value.free();
    }

    private void populateStatement(NamedParameterPreparedStatement preparedStatement, List<Object> dataList)
            throws SQLException {

        Iterator<Object> dataIterator = dataList.iterator();
        for (ColumnMetadata columnMetadata : getColumns(preparedStatement)) {
            final Object value = dataIterator.next();
            if (value == null) {
                preparedStatement.setNull(columnMetadata.getName(), columnMetadata.getType());
            } else {
                switch (columnMetadata.getType()) {
                    case Types.CLOB:
                        populateClob(preparedStatement, (Clob) value, columnMetadata.getName());
                        break;
                    case Types.BLOB:
                        populateBlob(preparedStatement, (Blob) value, columnMetadata.getName());
                        break;
                    case Types.SQLXML:
                        populateSqlXml(preparedStatement, (SQLXML) value, columnMetadata.getName());
                        break;
                    default:
                        preparedStatement.setObject(columnMetadata.getName(), value);
                }
            }
        }
    }

    private List<ColumnMetadata> getColumns(NamedParameterPreparedStatement preparedStatement) {

        if (columnsMetadata2Process == null) {
            columnsMetadata2Process = new ArrayList<>(dataTransfer.getRowMetadata().getColumnCount());

            for (ColumnMetadata columnMetadata : dataTransfer.getRowMetadata().getColumnsMetadata()) {
                if (preparedStatement.hasNamedParameter(columnMetadata.getName())) {
                    columnsMetadata2Process.add(columnMetadata);
                } else {
                    LOGGER.warn("Column {} not used", columnMetadata.getName());
                }
            }
        }

        return columnsMetadata2Process;
    }
}
