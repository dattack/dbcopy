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
import com.dattack.dbcopy.engine.datatype.AbstractDataType;
import com.dattack.dbcopy.engine.datatype.BigDecimalType;
import com.dattack.dbcopy.engine.datatype.BlobType;
import com.dattack.dbcopy.engine.datatype.BooleanType;
import com.dattack.dbcopy.engine.datatype.ByteType;
import com.dattack.dbcopy.engine.datatype.BytesType;
import com.dattack.dbcopy.engine.datatype.ClobType;
import com.dattack.dbcopy.engine.datatype.DataTypeVisitor;
import com.dattack.dbcopy.engine.datatype.DateType;
import com.dattack.dbcopy.engine.datatype.DoubleType;
import com.dattack.dbcopy.engine.datatype.FloatType;
import com.dattack.dbcopy.engine.datatype.IntegerType;
import com.dattack.dbcopy.engine.datatype.LongType;
import com.dattack.dbcopy.engine.datatype.NClobType;
import com.dattack.dbcopy.engine.datatype.NStringType;
import com.dattack.dbcopy.engine.datatype.NullType;
import com.dattack.dbcopy.engine.datatype.ShortType;
import com.dattack.dbcopy.engine.datatype.StringType;
import com.dattack.dbcopy.engine.datatype.TimeType;
import com.dattack.dbcopy.engine.datatype.TimestampType;
import com.dattack.dbcopy.engine.datatype.XmlType;
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
import java.sql.BatchUpdateException;
import java.sql.Blob;
import java.sql.Clob;
import java.sql.Connection;
import java.sql.NClob;
import java.sql.SQLException;
import java.sql.SQLXML;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.Callable;

/**
 * Executes the INSERT operations.
 *
 * @author cvarela
 * @since 0.1
 */
class InsertOperation implements Callable<Integer> {

    private static final Logger LOGGER = LoggerFactory.getLogger(InsertOperation.class);

    private final transient InsertOperationBean bean;
    private final transient AbstractConfiguration configuration;
    private final transient DataTransfer dataTransfer;
    private final transient DbCopyTaskResult taskResult;
    private transient volatile List<ColumnMetadata> columnsMetadata2Process;
    private transient Connection connection;
    private transient NamedParameterPreparedStatement preparedStatement;
    private transient int rowNumber;

    public InsertOperation(final InsertOperationBean bean, final DataTransfer dataTransfer,
                           final AbstractConfiguration configuration, final DbCopyTaskResult taskResult) {
        this.bean = bean;
        this.dataTransfer = dataTransfer;
        this.configuration = configuration;
        this.taskResult = taskResult;
        this.rowNumber = 0;
    }

    @Override
    public Integer call() throws Exception {

        final Visitor visitor = new Visitor();
        int totalInsertedRows = 0;

        while (true) {
            final AbstractDataType<?>[] row = dataTransfer.transfer();
            if (Objects.isNull(row)) {
                break;
            }

            for (final ColumnMetadata columnMetadata : getColumns(getPreparedStatement())) {
                visitor.set(columnMetadata, row[columnMetadata.getIndex() - 1]);
            }

            totalInsertedRows += execute();
        }

        totalInsertedRows += flush();

        return totalInsertedRows;
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

    /* default */ NamedParameterPreparedStatement getPreparedStatement() throws SQLException {
        if (preparedStatement == null || preparedStatement.isClosed()) {
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

    private int addBatch() throws SQLException {
        getPreparedStatement().addBatch();
        rowNumber++;
        int insertedRows = 0;
        if (rowNumber % bean.getBatchSize() == 0) {
            long startTime = 0;
            if (LOGGER.isDebugEnabled()) {
                startTime = System.currentTimeMillis();
            }
            insertedRows = executeBatch();
            if (LOGGER.isDebugEnabled()) {
                LOGGER.debug("{} rows inserted in {} ms. (total rows: {})",
                        String.format("%,d", insertedRows),
                        String.format("%,d", System.currentTimeMillis() - startTime),
                        String.format("%,d", rowNumber));
            }
        }
        return insertedRows;
    }

    private String createAutomapSql() {
        final StringBuilder columns = new StringBuilder();
        final StringBuilder refs = new StringBuilder();

        String concat = "";

        for (final ColumnMetadata columnMetadata : dataTransfer.getRowMetadata().getColumnsMetadata()) {
            columns.append(concat).append(columnMetadata.getName());
            refs.append(concat).append(':').append(columnMetadata.getName());
            concat = ",";
        }

        return String.format("INSERT INTO %s(%s) VALUES (%s)", bean.getTable(), columns, refs);
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

    private int executeBatch() throws SQLException {

        int insertedRows = 0;
        try {
            final int[] batchResult = getPreparedStatement().executeBatch();

            for (final int result : batchResult) {
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

    private List<ColumnMetadata> getColumns(final NamedParameterPreparedStatement preparedStatement) {

        if (Objects.isNull(columnsMetadata2Process)) {
            columnsMetadata2Process = new ArrayList<>(dataTransfer.getRowMetadata().getColumnCount());

            for (final ColumnMetadata columnMetadata : dataTransfer.getRowMetadata().getColumnsMetadata()) {
                if (preparedStatement.hasNamedParameter(columnMetadata.getName())) {
                    columnsMetadata2Process.add(columnMetadata);
                } else {
                    LOGGER.warn("Column {} not used", columnMetadata.getName());
                }
            }
        }

        return columnsMetadata2Process;
    }

    private synchronized Connection getConnection() throws SQLException {
        if (connection == null || connection.isClosed()) {
            connection = new JNDIDataSource(ConfigurationUtil.interpolate(bean.getDatasource(), configuration))
                    .getConnection();
            if (bean.getBatchSize() > 0) {
                connection.setAutoCommit(false);
            }
        }
        return connection;
    }

    /**
     * Default {@link DataTypeVisitor} implementation.
     */
    private class Visitor implements DataTypeVisitor { //NOPMD

        private transient ColumnMetadata columnMetadata;

        public void setColumnMetadata(final ColumnMetadata columnMetadata) {
            this.columnMetadata = columnMetadata;
        }

        @Override
        public void visit(final BigDecimalType type) throws SQLException {
            getPreparedStatement().setBigDecimal(columnMetadata.getName(), type.getValue());
        }

        @Override
        public void visit(final BlobType type) throws SQLException {

            final Blob targetBlob = getPreparedStatement().getConnection().createBlob();
            try (OutputStream output = targetBlob.setBinaryStream(1);
                 InputStream input = type.getValue().getBinaryStream()) {

                final byte[] buffer = new byte[1024]; // TODO: configurable size
                int len;
                while ((len = input.read(buffer)) != -1) {
                    output.write(buffer, 0, len);
                }

                getPreparedStatement().setBlob(columnMetadata.getName(), targetBlob);
            } catch (IOException e) {
                throw new SQLException("Unable to create Clob object: " + e.getMessage(), e);
            }
        }

        @Override
        public void visit(final BooleanType type) throws SQLException {
            getPreparedStatement().setBoolean(columnMetadata.getName(), type.getValue());
        }

        @Override
        public void visit(final ByteType type) throws SQLException {
            getPreparedStatement().setByte(columnMetadata.getName(), type.getValue());
        }

        @Override
        public void visit(final BytesType type) throws SQLException {
            getPreparedStatement().setBytes(columnMetadata.getName(), type.getValue());
        }

        @Override
        public void visit(final ClobType type) throws SQLException {

            final Clob targetClob = getPreparedStatement().getConnection().createClob();
            try (Writer clobWriter = targetClob.setCharacterStream(1)) {
                clobWriter.write(type.getValue().getSubString(1L, (int) type.getValue().length()));
                getPreparedStatement().setClob(columnMetadata.getName(), targetClob);
            } catch (IOException e) {
                throw new SQLException("Unable to create Clob object: " + e.getMessage(), e);
            }
        }

        @Override
        public void visit(final DateType type) throws SQLException {
            getPreparedStatement().setDate(columnMetadata.getName(), type.getValue());
        }

        @Override
        public void visit(final DoubleType type) throws SQLException {
            getPreparedStatement().setDouble(columnMetadata.getName(), type.getValue());
        }

        @Override
        public void visit(final FloatType type) throws SQLException {
            getPreparedStatement().setFloat(columnMetadata.getName(), type.getValue());
        }

        @Override
        public void visit(final IntegerType type) throws SQLException {
            getPreparedStatement().setInt(columnMetadata.getName(), type.getValue());
        }

        @Override
        public void visit(final LongType type) throws SQLException {
            getPreparedStatement().setLong(columnMetadata.getName(), type.getValue());
        }

        @Override
        public void visit(final NClobType type) throws SQLException {

            final NClob targetClob = getPreparedStatement().getConnection().createNClob();
            try (Writer clobWriter = targetClob.setCharacterStream(1)) {
                clobWriter.write(type.getValue().getSubString(1L, (int) type.getValue().length()));
                getPreparedStatement().setClob(columnMetadata.getName(), targetClob);
            } catch (IOException e) {
                throw new SQLException("Unable to create Clob object: " + e.getMessage(), e);
            }
        }

        @Override
        public void visit(final NStringType type) throws SQLException {
            getPreparedStatement().setNString(columnMetadata.getName(), type.getValue());
        }

        @Override
        public void visit(final NullType type) throws SQLException {
            getPreparedStatement().setNull(columnMetadata.getName(), columnMetadata.getType());
        }

        @Override
        public void visit(final ShortType type) throws SQLException {
            getPreparedStatement().setShort(columnMetadata.getName(), type.getValue());
        }

        @Override
        public void visit(final StringType type) throws SQLException {
            getPreparedStatement().setString(columnMetadata.getName(), type.getValue());
        }

        @Override
        public void visit(final TimeType type) throws SQLException {
            getPreparedStatement().setTime(columnMetadata.getName(), type.getValue());
        }

        @Override
        public void visit(final TimestampType type) throws SQLException {
            getPreparedStatement().setTimestamp(columnMetadata.getName(), type.getValue());
        }

        @Override
        public void visit(final XmlType type) throws SQLException {
            final SQLXML targetXml = getPreparedStatement().getConnection().createSQLXML();
            targetXml.setString(type.getValue().getString());
            getPreparedStatement().setSQLXML(columnMetadata.getName(), targetXml);
        }

        private void set(final ColumnMetadata columnMetadata, final AbstractDataType<?> value) throws Exception {
            this.columnMetadata = columnMetadata;
            if (Objects.isNull(value) || value.isNull()) {
                getPreparedStatement().setNull(columnMetadata.getName(), columnMetadata.getType());
            } else {
                value.accept(this);
            }
        }
    }
}
