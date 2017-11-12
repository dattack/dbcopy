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

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;

import javax.sql.DataSource;

import org.apache.commons.configuration.AbstractConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.dattack.dbcopy.beans.DbcopyJobBean;
import com.dattack.dbcopy.beans.InsertOperationBean;
import com.dattack.jtoolbox.commons.configuration.ConfigurationUtil;
import com.dattack.jtoolbox.jdbc.JNDIDataSource;

/**
 * @author cvarela
 * @since 0.1
 */
class DbCopyTask implements Callable<DbCopyTaskResult> {

    private static final Logger LOGGER = LoggerFactory.getLogger(DbCopyTask.class);

    private final AbstractConfiguration configuration;
    private final DbcopyJobBean dbcopyJobBean;
    private final DbCopyTaskResult taskResult;

    private static void flush(final List<InsertOperationContext> insertContextList, final DbCopyTaskResult taskResult) {
        if (insertContextList != null) {
            for (final InsertOperationContext context : insertContextList) {
                try {
                    taskResult.addInsertedRows(context.flush());
                } catch (final SQLException e) {
                    LOGGER.warn("Unable to complete flush operation", e);
                }
            }
        }
    }

    public DbCopyTask(final DbcopyJobBean dbcopyJobBean, final AbstractConfiguration configuration,
            final DbCopyTaskResult taskResult) {
        this.dbcopyJobBean = dbcopyJobBean;
        this.configuration = configuration;
        this.taskResult = taskResult;
    }

    @Override
    public DbCopyTaskResult call() {

        taskResult.start();

        LOGGER.info("DBCopy task started {} (Thread: {})", taskResult.getTaskName(), Thread.currentThread().getName());

        final String compiledSql = compileSql();
        LOGGER.debug("Executing SQL: {}", compiledSql);

        List<InsertOperationContext> insertContextList = null;
        try (Connection selectConn = getDataSource().getConnection(); //
                Statement selectStmt = selectConn.createStatement(); //
                ResultSet resultSet = selectStmt.executeQuery(compiledSql); //
        ) {

            insertContextList = createInsertContext(resultSet);

            while (resultSet.next()) {
                taskResult.incrementRetrievedRows();
                insertRow(insertContextList);
            }

            LOGGER.info("DBCopy task finished {}", taskResult.getTaskName());

        } catch (final SQLException e) {
            LOGGER.error("DBCopy task failed {}", taskResult.getTaskName());
            taskResult.setException(e);
        } finally {
            flush(insertContextList, taskResult);
        }

        taskResult.end();
        return taskResult;
    }

    private String compileSql() {
        return ConfigurationUtil.interpolate(dbcopyJobBean.getSelectBean().getSql(), configuration);
    }

    private List<InsertOperationContext> createInsertContext(final ResultSet resultSet) throws SQLException {

        final List<InsertOperationContext> insertContextList = new ArrayList<>(dbcopyJobBean.getInsertBean().size());
        for (final InsertOperationBean item : dbcopyJobBean.getInsertBean()) {
            insertContextList.add(new InsertOperationContext(item, resultSet));
        }
        return insertContextList;
    }

    private DataSource getDataSource() {
        return new JNDIDataSource(dbcopyJobBean.getSelectBean().getDatasource());
    }

    private void insertRow(final List<InsertOperationContext> insertContextList) {

        for (final InsertOperationContext context : insertContextList) {
            try {
                taskResult.addInsertedRows(context.insert());
            } catch (final SQLException e) {
                LOGGER.error("Insert failed for {}: {} (SQLSTATE: {}, Error code: {})", taskResult.getTaskName(),
                        e.getMessage(), e.getSQLState(), e.getErrorCode());
            }
        }
    }
}
