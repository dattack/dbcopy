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
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicInteger;

import javax.sql.DataSource;

import org.apache.commons.configuration.AbstractConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.dattack.dbcopy.beans.DbcopyJobBean;
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
    private final ExecutionController executionController;
    private static final AtomicInteger sequence = new AtomicInteger();

    public DbCopyTask(final DbcopyJobBean dbcopyJobBean, final AbstractConfiguration configuration,
            final DbCopyTaskResult taskResult) {
        this.dbcopyJobBean = dbcopyJobBean;
        this.configuration = configuration;
        this.taskResult = taskResult;
        int poolSize = dbcopyJobBean.getInsertBean() == null ? 1 : dbcopyJobBean.getInsertBean().getParallel();
        executionController = new ExecutionController("Writer-" + dbcopyJobBean.getId(),
                poolSize, poolSize);
        MBeanHelper.registerMBean("com.dattack.dbcopy:type=ThreadPool,name=Writer-" + dbcopyJobBean.getId() + "-"
                + sequence.getAndIncrement(), executionController);
    }

    private Statement createStatement(Connection connection) throws SQLException {
        Statement stmt = connection.createStatement();
        if (dbcopyJobBean.getSelectBean().getFetchSize() > 0) {
            stmt.setFetchSize(dbcopyJobBean.getSelectBean().getFetchSize());
        }
        return stmt;
    }

    @Override
    public DbCopyTaskResult call() {

        taskResult.start();

        LOGGER.info("DBCopy task started {} (Thread: {})", taskResult.getTaskName(), Thread.currentThread().getName());

        final String compiledSql = compileSql();
        LOGGER.info("Executing SQL: {}", compiledSql);

        try (Connection selectConn = getDataSource().getConnection(); //
                Statement selectStmt = createStatement(selectConn); //
                ResultSet resultSet = selectStmt.executeQuery(compiledSql); //
        ) {

            DataProvider dataProvider = new DataProvider(resultSet, taskResult);

            final List<Future<?>> futureList = new ArrayList<>();

            if (dbcopyJobBean.getInsertBean() != null) {
                for (int i = 0; i < dbcopyJobBean.getInsertBean().getParallel(); i++) {
                    futureList.add(executionController.submit(new InsertOperationContext(dbcopyJobBean.getInsertBean(),
                            dataProvider, configuration, taskResult)));
                }
            }

            if (dbcopyJobBean.getExportBean() != null) {
                futureList.add(executionController.submit(new ExportOperation(dbcopyJobBean.getExportBean(),
                        dataProvider,  configuration, taskResult)));
            }

            executionController.shutdown();
            showFutures(futureList);
            LOGGER.info("DBCopy task finished {}", taskResult.getTaskName());

        } catch (final SQLException e) {
            LOGGER.error("DBCopy task failed {}", taskResult.getTaskName());
            taskResult.setException(e);
        }

        taskResult.end();
        return taskResult;
    }

    private static void showFutures(final List<Future<?>> futureList) {

        for (final Future<?> future : futureList) {
            try {
                LOGGER.info("Future result: {}", future.get());
            } catch (final InterruptedException | ExecutionException e) {
                LOGGER.warn("Error getting computed result from Future object", e);
            }
        }
    }

    private String compileSql() {
        return ConfigurationUtil.interpolate(dbcopyJobBean.getSelectBean().getSql(), configuration);
    }

    private DataSource getDataSource() {
        return new JNDIDataSource(
                ConfigurationUtil.interpolate(dbcopyJobBean.getSelectBean().getDatasource(), configuration));
    }
}
