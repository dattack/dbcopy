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
import com.dattack.dbcopy.engine.datatype.*;
import com.dattack.jtoolbox.commons.configuration.ConfigurationUtil;
import com.dattack.jtoolbox.jdbc.JDBCUtils;
import com.dattack.jtoolbox.jdbc.JNDIDataSource;
import com.dattack.jtoolbox.jdbc.NamedParameterPreparedStatement;
import org.apache.commons.configuration.AbstractConfiguration;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.Writer;
import java.sql.*;
import java.util.ArrayList;
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
    private final DbCopyTaskResult taskResult;
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

        taskResult.addProcessedRows(insertedRows);

        JDBCUtils.closeQuietly(preparedStatement);
        JDBCUtils.closeQuietly(getConnection());

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

    private String createAutomapSql() {
        StringBuilder columns = new StringBuilder();
        StringBuilder refs = new StringBuilder();

        String concat = "";

        for (ColumnMetadata columnMetadata: dataTransfer.getRowMetadata().getColumnsMetadata()) {
            columns.append(concat).append(columnMetadata.getName());
            refs.append(concat).append(":").append(columnMetadata.getName());
            concat = ",";
        }

        return String.format("INSERT INTO %s(%s) VALUES (%s)", bean.getTable(), columns.toString(), refs.toString());
    }

    private NamedParameterPreparedStatement getPreparedStatement() throws SQLException {
        if (preparedStatement == null) {
            String sql;
            if (StringUtils.isNotBlank(bean.getSql())) {
                sql = bean.getSql();
            } else if (StringUtils.isNotBlank(bean.getTable())) {
                sql = createAutomapSql();
            } else {
                throw new SQLException("Missing insert statement or table name");
            }

            LOGGER.info(sql);
            preparedStatement = NamedParameterPreparedStatement.build(getConnection(),
                    ConfigurationUtil.interpolate(sql, configuration));
        }
        return preparedStatement;
    }

    private int execute() throws SQLException {

        int insertedRows;

        if (bean.getBatchSize() > 0) {
            insertedRows = addBatch();
        } else {
            insertedRows = getPreparedStatement().executeUpdate();
        }

        taskResult.addProcessedRows(insertedRows);

        return insertedRows;
    }
    @Override
    public Integer call() throws Exception {

        Visitor visitor = new Visitor();
        int totalInsertedRows = 0;

        while (true) {
            AbstractDataType<?>[] row = dataTransfer.transfer();
            if (row == null) {
                break;
            }

            for (ColumnMetadata columnMetadata : getColumns(getPreparedStatement())) {
                visitor.set(columnMetadata, row[columnMetadata.getIndex() - 1]);
            }

            totalInsertedRows += execute();
        }

        totalInsertedRows += flush();

        return totalInsertedRows;
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

    private class Visitor implements DataTypeVisitor {

        private ColumnMetadata columnMetadata;

        public void setColumnMetadata(ColumnMetadata columnMetadata) {
            this.columnMetadata = columnMetadata;
        }

        @Override
        public void visit(BigDecimalType type) throws SQLException {
            getPreparedStatement().setBigDecimal(columnMetadata.getName(), type.getValue());
        }

        @Override
        public void visit(BlobType type) throws SQLException {

            try {
                Blob targetBlob = getPreparedStatement().getConnection().createBlob();
                OutputStream output = targetBlob.setBinaryStream(1);

                InputStream in = type.getValue().getBinaryStream();
                byte[] buffer = new byte[1024]; // TODO: configurable size
                int len;
                while ((len = in.read(buffer)) != -1) {
                    output.write(buffer, 0, len);
                }

                getPreparedStatement().setBlob(columnMetadata.getName(), targetBlob);
            } catch (IOException e) {
                throw new SQLException("Unable to create Clob object: " + e.getMessage());
            }
        }

        @Override
        public void visit(BooleanType type) throws SQLException {
            getPreparedStatement().setBoolean(columnMetadata.getName(), type.getValue());
        }

        @Override
        public void visit(ByteType type) throws SQLException {
            getPreparedStatement().setByte(columnMetadata.getName(), type.getValue());
        }

        @Override
        public void visit(BytesType type) throws SQLException {
            getPreparedStatement().setBytes(columnMetadata.getName(), type.getValue());
        }

        @Override
        public void visit(ClobType type) throws SQLException {
            try {
                Clob targetClob = getPreparedStatement().getConnection().createClob();
                Writer clobWriter = targetClob.setCharacterStream(1);
                clobWriter.write(type.getValue().getSubString(0L, (int) type.getValue().length()));
                getPreparedStatement().setClob(columnMetadata.getName(), targetClob);
            } catch (IOException e) {
                throw new SQLException("Unable to create Clob object: " + e.getMessage());
            }
        }

        @Override
        public void visit(DateType type) throws SQLException {
            getPreparedStatement().setDate(columnMetadata.getName(), type.getValue());
        }

        @Override
        public void visit(DoubleType type) throws SQLException {
            getPreparedStatement().setDouble(columnMetadata.getName(), type.getValue());
        }

        @Override
        public void visit(FloatType type) throws SQLException {
            getPreparedStatement().setFloat(columnMetadata.getName(), type.getValue());
        }

        @Override
        public void visit(IntegerType type) throws SQLException {
            getPreparedStatement().setInt(columnMetadata.getName(), type.getValue());
        }

        @Override
        public void visit(LongType type) throws SQLException {
            getPreparedStatement().setLong(columnMetadata.getName(), type.getValue());
        }

        @Override
        public void visit(NClobType type) throws SQLException {
            getPreparedStatement().setNClob(columnMetadata.getName(), type.getValue());
        }

        @Override
        public void visit(NStringType type) throws SQLException {
            getPreparedStatement().setNString(columnMetadata.getName(), type.getValue());
        }

        @Override
        public void visit(NullType type) throws SQLException {
            getPreparedStatement().setNull(columnMetadata.getName(), columnMetadata.getType());
        }

        @Override
        public void visit(ShortType type) throws SQLException {
            getPreparedStatement().setShort(columnMetadata.getName(), type.getValue());
        }

        @Override
        public void visit(StringType type) throws SQLException {
            getPreparedStatement().setString(columnMetadata.getName(), type.getValue());
        }

        @Override
        public void visit(TimeType type) throws SQLException {
            getPreparedStatement().setTime(columnMetadata.getName(), type.getValue());
        }

        @Override
        public void visit(TimestampType type) throws SQLException {
            getPreparedStatement().setTimestamp(columnMetadata.getName(), type.getValue());
        }

        @Override
        public void visit(XmlType type) throws SQLException {
            SQLXML targetXml = getPreparedStatement().getConnection().createSQLXML();
            targetXml.setString(type.getValue().getString());
            getPreparedStatement().setSQLXML(columnMetadata.getName(), targetXml);
        }

        private void set(ColumnMetadata columnMetadata, AbstractDataType<?> value) throws Exception {
            this.columnMetadata = columnMetadata;
            if (value == null || value.isNull()) {
                getPreparedStatement().setNull(columnMetadata.getName(), columnMetadata.getType());
            } else {
                value.accept(this);
            }
        }
    }
}
