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

import com.dattack.dbcopy.beans.DbcopyTaskBean;
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
    private final DbcopyTaskBean dbcopyTaskBean;
    private final RangeValue rangeValue;

    public DbCopyTask(final DbcopyTaskBean dbcopyTaskBean, final AbstractConfiguration configuration,
            final RangeValue rangeValue) {
        this.dbcopyTaskBean = dbcopyTaskBean;
        this.configuration = configuration;
        this.rangeValue = rangeValue;
    }

    @Override
    public DbCopyTaskResult call() {

        LOGGER.info("DBCopy task started for {} (Thread: {})", rangeValue, Thread.currentThread().getName());

        final List<InsertOperationContext> insertContextList = createInsertContext();

        final String compiledSql = compileSql();
        LOGGER.debug("Executing SQL: {}", compiledSql);

        final DbCopyTaskResult taskResult = new DbCopyTaskResult(rangeValue);

        try (Connection selectConn = getDataSource().getConnection(); //
                Statement selectStmt = selectConn.createStatement(); //
                ResultSet resultSet = selectStmt.executeQuery(compiledSql); //
        ) {

            while (resultSet.next()) {
                taskResult.addRetrievedRows(1);
                for (final InsertOperationContext context : insertContextList) {
                    try {
                        taskResult.addInsertedRows(context.insert(resultSet));
                    } catch (final SQLException e) {
                        LOGGER.error("Insert failed for {}: {} (SQLSTATE: {}, Error code: {})", rangeValue,
                                e.getMessage(), e.getSQLState(), e.getErrorCode());
                    }
                }
            }

            LOGGER.info("DBCopy task finished for {}", rangeValue);

        } catch (final SQLException e) {
            LOGGER.error("DBCopy task failed for {}", rangeValue);
            taskResult.setException(e);
        } finally {
            for (final InsertOperationContext context : insertContextList) {
                try {
                    taskResult.addInsertedRows(context.flush());
                } catch (final SQLException e) {
                    e.printStackTrace();
                }
            }
        }
        return taskResult;
    }

    private String compileSql() {
        return ConfigurationUtil.interpolate(dbcopyTaskBean.getSelectBean().getSql(), configuration);
    }

    private List<InsertOperationContext> createInsertContext() {

        final List<InsertOperationContext> insertContextList = new ArrayList<>(dbcopyTaskBean.getInsertBean().size());
        for (final InsertOperationBean item : dbcopyTaskBean.getInsertBean()) {
            insertContextList.add(new InsertOperationContext(item));
        }
        return insertContextList;
    }

    private DataSource getDataSource() {
        return new JNDIDataSource(dbcopyTaskBean.getSelectBean().getDatasource());
    }
}
