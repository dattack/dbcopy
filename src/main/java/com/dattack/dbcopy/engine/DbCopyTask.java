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

import com.dattack.dbcopy.beans.DbcopyJobBean;
import com.dattack.jtoolbox.commons.configuration.ConfigurationUtil;
import com.dattack.jtoolbox.io.IOUtils;
import com.dattack.jtoolbox.jdbc.JNDIDataSource;
import org.apache.commons.configuration.AbstractConfiguration;
import org.apache.commons.lang.ObjectUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.sql.DataSource;
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

/**
 * @author cvarela
 * @since 0.1
 */
class DbCopyTask implements Callable<DbCopyTaskResult> {

    private static final Logger LOGGER = LoggerFactory.getLogger(DbCopyTask.class);
    private static final AtomicInteger sequence = new AtomicInteger();
    private final AbstractConfiguration configuration;
    private final DbcopyJobBean dbcopyJobBean;
    private final DbCopyTaskResult taskResult;

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
        LOGGER.info("Executing SQL: {}", compiledSql);

        try (Connection selectConn = getDataSource().getConnection(); //
             Statement selectStmt = createStatement(selectConn); //
             ResultSet resultSet = selectStmt.executeQuery(compiledSql) //
        ) {

            DataTransfer dataTransfer =
                    new DataTransfer(resultSet, taskResult, //
                            dbcopyJobBean.getSelectBean().getFetchSize());

            final List<Future<?>> futureList = new ArrayList<>();
            futureList.addAll(createInsertFutures(dataTransfer));
            futureList.addAll(createExportFutures(dataTransfer));

            showFutures(futureList);
            LOGGER.info("DBCopy task finished {}", taskResult.getTaskName());

        } catch (final Exception e) {
            LOGGER.error("DBCopy task failed {}", taskResult.getTaskName());
            taskResult.setException(e);
        }

        taskResult.end();
        return taskResult;
    }

    private String compileSql() {
        return ConfigurationUtil.interpolate(dbcopyJobBean.getSelectBean().getSql(), configuration);
    }

    private DataSource getDataSource() {
        return new JNDIDataSource(
                ConfigurationUtil.interpolate(dbcopyJobBean.getSelectBean().getDatasource(), configuration));
    }

    private Statement createStatement(Connection connection) throws SQLException {
        Statement stmt = connection.createStatement();
        if (dbcopyJobBean.getSelectBean().getFetchSize() > 0) {
            stmt.setFetchSize(dbcopyJobBean.getSelectBean().getFetchSize());
        }
        return stmt;
    }

    private List<Future<?>> createInsertFutures(DataTransfer dataTransfer) {

        final List<Future<?>> futureList = new ArrayList<>();

        if (dbcopyJobBean.getInsertBean() != null) {

            int poolSize = dbcopyJobBean.getInsertBean().getParallel();
            ExecutionController executionController = new ExecutionController("Insert-" + dbcopyJobBean.getId(),
                    poolSize, poolSize);
            MBeanHelper.registerMBean("com.dattack.dbcopy:type=ThreadPool,name=Insert-" + dbcopyJobBean.getId() + "-"
                    + sequence.getAndIncrement(), executionController);

            for (int i = 0; i < dbcopyJobBean.getInsertBean().getParallel(); i++) {
                futureList.add(executionController.submit(new InsertOperation(dbcopyJobBean.getInsertBean(),
                        dataTransfer, configuration, taskResult)));
            }

            executionController.shutdown();
        }

        return futureList;
    }

    private List<Future<?>> createExportFutures(DataTransfer dataTransfer) {

        final List<Future<?>> futureList = new ArrayList<>();

        if (dbcopyJobBean.getExportBean() != null) {

            int poolSize = dbcopyJobBean.getExportBean().getParallel();
            ExecutionController executionController = new ExecutionController("Export-" + dbcopyJobBean.getId(),
                    poolSize, poolSize);
            MBeanHelper.registerMBean("com.dattack.dbcopy:type=ThreadPool,name=Export-" + dbcopyJobBean.getId() + "-"
                    + sequence.getAndIncrement(), executionController);


            ExportWriteWrapper writer = new ExportWriteWrapper(dbcopyJobBean.getExportBean(), configuration);
            taskResult.addOnEndCommand(() -> {
                    IOUtils.closeQuietly(writer);
                    return null; }
            );

            for (int i = 0; i < dbcopyJobBean.getExportBean().getParallel(); i++) {
                futureList.add(executionController.submit(new ExportOperation(dbcopyJobBean.getExportBean(),
                        dataTransfer, taskResult, writer)));
            }

            executionController.shutdown();
        }

        return futureList;
    }

    private void showFutures(final List<Future<?>> futureList) {

        for (final Future<?> future : futureList) {
            try {
                LOGGER.info("Future result: {}", future.get());
            } catch (final InterruptedException | ExecutionException e) {
                LOGGER.warn("Error getting computed result from Future object", e);
                taskResult.setException(e);
            }
        }
    }
}
