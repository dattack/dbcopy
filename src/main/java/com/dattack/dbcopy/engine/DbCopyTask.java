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
public class DbCopyTask implements Runnable {

    private static final Logger LOGGER = LoggerFactory.getLogger(DbCopyTask.class);

    private final AbstractConfiguration configuration;
    private final DbcopyTaskBean dbcopyTaskBean;

    public DbCopyTask(final DbcopyTaskBean dbcopyTaskBean, final AbstractConfiguration configuration) {
        this.dbcopyTaskBean = dbcopyTaskBean;
        this.configuration = configuration;
    }

    private List<InsertOperationContext> createInsertContext() {

        final List<InsertOperationContext> insertContextList = new ArrayList<>(dbcopyTaskBean.getInsertBean().size());
        for (final InsertOperationBean item : dbcopyTaskBean.getInsertBean()) {
            insertContextList.add(new InsertOperationContext(item));
        }
        return insertContextList;
    }

    @Override
    public void run() {

        final DataSource selectDS = new JNDIDataSource(dbcopyTaskBean.getSelectBean().getDatasource());

        final List<InsertOperationContext> insertContextList = createInsertContext();

        final String sql = ConfigurationUtil.interpolate(dbcopyTaskBean.getSelectBean().getSql(), configuration);
        LOGGER.info("Executing SQL: {}", sql);

        try (Connection selectConn = selectDS.getConnection(); //
                Statement selectStmt = selectConn.createStatement(); //
                ResultSet rs = selectStmt.executeQuery(sql); //
        ) {

            while (rs.next()) {
                for (final InsertOperationContext context : insertContextList) {
                    context.insert(rs);
                }
            }

            LOGGER.info("SQL finished: {}", sql);

        } catch (final SQLException e) {
            LOGGER.error("Error reading or writing data: {}", e.getMessage());
        } finally {
            for (final InsertOperationContext context : insertContextList) {
                try {
                    context.close();
                } catch (final SQLException e) {
                    LOGGER.warn("Unable to close context: {} -> {}", e.getMessage(), e.getCause());
                }
            }
        }
    }
}
