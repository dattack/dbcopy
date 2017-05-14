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
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.dattack.dbcopy.beans.InsertOperationBean;
import com.dattack.jtoolbox.jdbc.JNDIDataSource;

public class InsertOperationContext implements AutoCloseable {

    private static final Logger LOGGER = LoggerFactory.getLogger(InsertOperationContext.class);

    private final InsertOperationBean bean;
    private Connection connection;
    private PreparedStatement preparedStatement;
    private int row;

    public InsertOperationContext(final InsertOperationBean bean) {
        this.bean = bean;
        this.row = 0;
    }

    public synchronized Connection getConnection() throws SQLException {
        if (connection == null) {
            connection = new JNDIDataSource(bean.getDatasource()).getConnection();
            if (bean.getBatchSize() > 0) {
                connection.setAutoCommit(false);
            }
        }
        return connection;
    }

    public synchronized PreparedStatement getPreparedStatement() throws SQLException {
        if (preparedStatement == null) {
            preparedStatement = getConnection().prepareStatement(bean.getSql());
        }
        return preparedStatement;
    }

    public void insert(final ResultSet rs) throws SQLException {

        for (int columnIndex = 1; columnIndex <= rs.getMetaData().getColumnCount(); columnIndex++) {
            getPreparedStatement().setObject(columnIndex, rs.getObject(columnIndex));
        }

        LOGGER.debug("{}", getPreparedStatement().toString());

        if (bean.getBatchSize() > 0) {
            getPreparedStatement().addBatch();
            row++;
            if (row % bean.getBatchSize() == 0) {
                executeBatch();
            }
        } else {
            getPreparedStatement().executeUpdate();
        }
    }

    private void executeBatch() throws SQLException {
        getPreparedStatement().executeBatch();
        getConnection().commit();

        LOGGER.debug("{}: {} rows", bean.getDatasource(), row);
    }

    @Override
    public void close() throws SQLException {

        if (row > 0 && row % bean.getBatchSize() != 0) {
            executeBatch();
        }

        if (preparedStatement != null) {
            preparedStatement.close();
        }

        if (connection != null) {
            connection.close();
        }
    }
}
