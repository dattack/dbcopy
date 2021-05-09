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

import com.dattack.dbcopy.engine.datatype.AbstractDataType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.ResultSet;
import java.sql.SQLException;
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
    private final int fetchSize;
    private final DbCopyTaskResult taskResult;
    private final TransferQueue<AbstractDataType<?>[]> transferQueue;
    private final Semaphore semaphore;
    private final ResultSet resultSet;
    private final RowMetadata rowMetadata;

    DataTransfer(ResultSet resultSet, DbCopyTaskResult taskResult, int fetchSize) throws SQLException {
        this.resultSet = resultSet;
        this.taskResult = taskResult;
        this.fetchSize = fetchSize > 0 ? fetchSize : DEFAULT_FETCH_SIZE;
        this.transferQueue = new LinkedTransferQueue<>();
        this.semaphore = new Semaphore(1);
        this.rowMetadata = createRowMetadata();
    }

    public AbstractDataType<?>[] transfer() throws SQLException, InterruptedException {

        AbstractDataType<?>[] row;

        boolean moreData = true;
        do {
            row = transferQueue.poll();

            if (row == null) {

                if (semaphore.tryAcquire(10, TimeUnit.MILLISECONDS)) {
                    LOGGER.trace("Semaphore acquired by thread '{}'", Thread.currentThread().getName());
                    int priority = Thread.currentThread().getPriority();
                    Thread.currentThread().setPriority(Thread.MAX_PRIORITY);
                    int counter = 0;
                    do {
                        row = publish();

                        if (row == null) {
                            moreData = false;
                            break;
                        }
                        transferQueue.put(row);
                        taskResult.incrementRetrievedRows();

                    } while (counter++ < fetchSize);
                    semaphore.release();
                    LOGGER.trace("Semaphore released by thread '{}'. Fetched rows {}",
                            Thread.currentThread().getName(), counter);
                    Thread.currentThread().setPriority(priority);
                }
                row = transferQueue.poll();
            }

        } while (row == null && moreData);

        return row;
    }


    public RowMetadata getRowMetadata() {
        return rowMetadata;
    }

    private RowMetadata createRowMetadata() throws SQLException {

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
        return rowMetadataBuilder.build();
    }

    private synchronized AbstractDataType<?>[] publish() throws SQLException {

        if (isResultSetClosedQuietly() || !resultSet.next()) {
            return null;
        }

        final AbstractDataType<?>[] dataList = new AbstractDataType[rowMetadata.getColumnCount()];
        for (final ColumnMetadata columnMetadata: rowMetadata.getColumnsMetadata()) {
            dataList[columnMetadata.getIndex() - 1] = columnMetadata.getFunction().get(resultSet);
        }

        return dataList;
    }

    private boolean isResultSetClosedQuietly() {
        try {
            return resultSet.isClosed();
        } catch (Throwable t) {
            // ignore
        }
        return false;
    }
}
