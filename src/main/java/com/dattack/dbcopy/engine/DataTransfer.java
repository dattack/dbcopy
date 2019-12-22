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

    private ResultSet resultSet;
    private DbCopyTaskResult taskResult;
    private RowMetadata rowMetadata;
    private TransferQueue<List<Object>> transferQueue;
    private Semaphore semaphore;

    DataTransfer(ResultSet resultSet, DbCopyTaskResult taskResult) throws SQLException {
        this.resultSet = resultSet;
        this.taskResult = taskResult;
        this.transferQueue = new LinkedTransferQueue<>();
        this.semaphore = new Semaphore(1);
        populateRowMetadata();
    }

    private void populateRowMetadata() throws SQLException {

        List<ColumnMetadata> columnMetadataList = new ArrayList<>(resultSet.getMetaData().getColumnCount());
        for (int columnIndex = 1; columnIndex <= resultSet.getMetaData().getColumnCount(); columnIndex++) {
            String columnName = resultSet.getMetaData().getColumnName(columnIndex);
            int columnType = resultSet.getMetaData().getColumnType(columnIndex);
            int scale = resultSet.getMetaData().getScale(columnIndex);
            columnMetadataList.add(new ColumnMetadata(columnName, columnIndex, columnType, scale));
        }
        rowMetadata = new RowMetadata(columnMetadataList);
    }

    public RowMetadata getRowMetadata() {
        return rowMetadata;
    }

    private boolean publish() throws SQLException, InterruptedException {

        if (resultSet.isClosed() || !resultSet.next()) {
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
                    Thread.currentThread().setPriority(Thread.MAX_PRIORITY);
                    int counter = 0;
                    do {
                        moreData = publish();
                    } while (moreData && counter++ < 100_000);
                    semaphore.release();
                    Thread.currentThread().setPriority(Thread.NORM_PRIORITY);
                    row = transferQueue.poll();
                }
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
