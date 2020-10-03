/*
 * Copyright (c) 2019, The Dattack team (http://www.dattack.com)
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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Clob;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Types;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.LinkedTransferQueue;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TransferQueue;

/**
 *
 * @author cvarela
 * @since 0.1
 */
public class DataTransfer {

    private final static Logger LOGGER = LoggerFactory.getLogger(DataTransfer.class);

    private static final int DEFAULT_FETCH_SIZE = 10_000;
    private int fetchSize;
    private ResultSet resultSet;
    private DbCopyTaskResult taskResult;
    private RowMetadata rowMetadata;
    private TransferQueue<List<Object>> transferQueue;
    private Semaphore semaphore;

    DataTransfer(ResultSet resultSet, DbCopyTaskResult taskResult, int fetchSize) throws SQLException {
        this.resultSet = resultSet;
        this.taskResult = taskResult;
        this.fetchSize = fetchSize > 0 ? fetchSize : DEFAULT_FETCH_SIZE;
        this.transferQueue = new LinkedTransferQueue<>();
        this.semaphore = new Semaphore(1);
        populateRowMetadata();
    }

    private void populateRowMetadata() throws SQLException {

        RowMetadata.RowMetadataBuilder rowMetadataBuilder = new RowMetadata.RowMetadataBuilder();

        for (int columnIndex = 1; columnIndex <= resultSet.getMetaData().getColumnCount(); columnIndex++) {
            ColumnMetadata columnMetadata = new ColumnMetadata.ColumnMetadataBuilder()
                    .withName(resultSet.getMetaData().getColumnName(columnIndex)) //
                    .withIndex(columnIndex) //
                    .withType(resultSet.getMetaData().getColumnType(columnIndex)) //
                    .withPrecision(resultSet.getMetaData().getPrecision(columnIndex)) //
                    .withScale(resultSet.getMetaData().getScale(columnIndex)) //
                    .withNullable(resultSet.getMetaData().isNullable(columnIndex)) //
                    .build();

            rowMetadataBuilder.add(columnMetadata);
        }
        rowMetadata = rowMetadataBuilder.build();
    }

    public RowMetadata getRowMetadata() {
        return rowMetadata;
    }

    private boolean isResultSetClosedQuietly() {
        try {
            return resultSet.isClosed();
        } catch (Throwable t) {
            // ignore
        }
        return false;
    }

    private boolean publish() throws SQLException, InterruptedException {

        if (isResultSetClosedQuietly() || !resultSet.next()) {
            return false;
        }

        List<Object> dataList = new ArrayList<>(rowMetadata.getColumnCount());
        for (ColumnMetadata columnMetadata: rowMetadata.getColumnsMetadata()) {
            dataList.add(retrieveData(columnMetadata));
        }

        transferQueue.put(dataList);
        taskResult.incrementRetrievedRows();

        return true;
    }

    public List<Object> transfer() throws SQLException, InterruptedException {

        List<Object> row;

        boolean moreData = true;
        do {
            row = transferQueue.poll();

            if (row == null) {

                if (semaphore.tryAcquire(10, TimeUnit.MILLISECONDS)) {
                    int priority = Thread.currentThread().getPriority();
                    Thread.currentThread().setPriority(Thread.MAX_PRIORITY);
                    int counter = 0;
                    do {
                        moreData = publish();
                    } while (moreData && counter++ < fetchSize);
                    semaphore.release();
                    Thread.currentThread().setPriority(priority);
                }
                row = transferQueue.poll();
            }

        } while (row == null && moreData);

        return row;
    }

    private Object retrieveData(ColumnMetadata columnMetadata) throws SQLException {

        Object value = resultSet.getObject(columnMetadata.getIndex());
        if (resultSet.wasNull()) {
            value = null;
        } else {

            switch (columnMetadata.getType()) {
                case Types.CLOB:
                    Clob sourceClob = resultSet.getClob(columnMetadata.getIndex());
                    value = sourceClob.getSubString(1L, (int) sourceClob.length());
                    break;

                case Types.BLOB:
                    value = resultSet.getBlob(columnMetadata.getIndex());
                    break;
                case Types.SQLXML:
                    value = resultSet.getSQLXML(columnMetadata.getIndex());
                    break;
                default:
            }
        }

        return value;
    }
}
