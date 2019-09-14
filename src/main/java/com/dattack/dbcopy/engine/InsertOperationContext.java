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

import java.sql.BatchUpdateException;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.concurrent.Callable;

import org.apache.commons.configuration.AbstractConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.dattack.dbcopy.beans.InsertOperationBean;
import com.dattack.jtoolbox.commons.configuration.ConfigurationUtil;
import com.dattack.jtoolbox.jdbc.JDBCUtils;
import com.dattack.jtoolbox.jdbc.JNDIDataSource;
import com.dattack.jtoolbox.jdbc.NamedParameterPreparedStatement;

/**
 * Executes the INSERT operations.
 *
 * @author cvarela
 * @since 0.1
 */
class InsertOperationContext implements Callable<Integer> {

    private final static Logger LOGGER = LoggerFactory.getLogger(InsertOperationContext.class);

    private final InsertOperationBean bean;
    private final DataProvider dataProvider;
    private final AbstractConfiguration configuration;
    private Connection connection;
    private NamedParameterPreparedStatement preparedStatement;
    private DbCopyTaskResult taskResult;
    private int rowNumber;

    public InsertOperationContext(final InsertOperationBean bean, final DataProvider dataProvider,
            final AbstractConfiguration configuration, DbCopyTaskResult taskResult) throws SQLException {
        this.bean = bean;
        this.dataProvider = dataProvider;
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

            for (int i = 0; i < batchResult.length; i++) {
                if (batchResult[i] > 0) {
                    insertedRows += batchResult[i];
                } else if (batchResult[i] == Statement.SUCCESS_NO_INFO) {
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
    public Integer call() throws SQLException {

        int totalInsertedRows = 0;
        while (dataProvider.populateStatement(getPreparedStatement())) {
            int insertedRows;
            if (bean.getBatchSize() > 0) {
                insertedRows = addBatch();
            } else {
                insertedRows = getPreparedStatement().executeUpdate();
            }
            taskResult.addInsertedRows(insertedRows);
            totalInsertedRows += insertedRows;
        }

        int insertedRows = flush();
        taskResult.addInsertedRows(insertedRows);

        totalInsertedRows += insertedRows;
        return totalInsertedRows;
    }
}
