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
import com.dattack.dbcopy.engine.functions.FunctionException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Objects;
import java.util.concurrent.LinkedTransferQueue;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TransferQueue;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Responsible for the control of the data transfer of a ResultSet.
 *
 * @author cvarela
 * @since 0.1
 */
public class DataTransfer {

    private static final int DEFAULT_FETCH_SIZE = 10_000;
    private static final Logger LOGGER = LoggerFactory.getLogger(DataTransfer.class);
    private final transient int fetchSize;
    private final transient ResultSet resultSet;
    private final transient RowMetadata rowMetadata;
    private final transient Semaphore semaphore;
    private final transient DbCopyTaskResult taskResult;
    private final transient TransferQueue<AbstractDataType<?>[]> transferQueue;
    private final AtomicInteger queueSize;
    private boolean exhausted = false;

    /* default */ DataTransfer(final ResultSet resultSet, final DbCopyTaskResult taskResult,
        final int fetchSize) throws SQLException
    {
        this.resultSet = resultSet;
        this.taskResult = taskResult;
        this.fetchSize = fetchSize > 0 ? fetchSize : DEFAULT_FETCH_SIZE;
        this.transferQueue = new LinkedTransferQueue<>();
        this.semaphore = new Semaphore(1);
        this.rowMetadata = createRowMetadata();
        this.queueSize = new AtomicInteger();
        MBeanHelper.registerMBean("TransferQueue", taskResult.getTaskName(), new TransferQueueWrapper(transferQueue));
    }

    public RowMetadata getRowMetadata() {
        return rowMetadata;
    }

    /**
     * Returns the next row of data or null if there is no more data.
     *
     * @return the next row of data or null if there is no more data.
     * @throws SQLException         if a database access error occurs or this method is called on a closed result set.
     * @throws InterruptedException if the current thread is interrupted
     * @throws FunctionException    if an error occurs when evaluating a function that provides data from source.
     */
    @SuppressWarnings("checkstyle:VariableDeclarationUsageDistance")
    public AbstractDataType<?>[] transfer() throws SQLException, InterruptedException, FunctionException {

        AbstractDataType<?>[] row;

        do {
            row = transferQueue.poll();

            if (row == null) {
                if (transferNextChunk(true) == 0) {
                    break;
                }
            } else if (queueSize.get() < fetchSize * 0.5) {
                transferNextChunk(false);
            }

        } while (Objects.isNull(row));

        queueSize.decrementAndGet();
        return row;
    }

    private RowMetadata createRowMetadata() throws SQLException {

        final RowMetadata.RowMetadataBuilder rowMetadataBuilder = RowMetadata.custom();

        for (int columnIndex = 1; columnIndex <= resultSet.getMetaData().getColumnCount(); columnIndex++) {
            final ColumnMetadata columnMetadata = ColumnMetadata.custom() //NOPMD
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

    private int transferNextChunk(boolean wait) throws InterruptedException, SQLException, FunctionException {
        int counter = -1;
        if (semaphore.tryAcquire()) {
            counter = 0;
            LOGGER.trace("Semaphore acquired by thread '{}'", Thread.currentThread().getName());
            final int previousPriority = Thread.currentThread().getPriority();
            Thread.currentThread().setPriority(Thread.MAX_PRIORITY);
            int limit = fetchSize - queueSize.get();
            do {
                AbstractDataType<?>[] publishedRow = publishNextRow();

                if (Objects.isNull(publishedRow)) {
                    if (counter <= 0) {
                        exhausted = true;
                    }
                    break;
                }

                transferQueue.put(publishedRow);
                taskResult.incrementRetrievedRows();
                queueSize.incrementAndGet();

                synchronized (this) {
                    notifyAll();
                }

            } while (counter++ < limit);
            semaphore.release();
            LOGGER.trace("Semaphore released by thread '{}'. Fetched rows {}", Thread.currentThread().getName(),
                         counter);
            synchronized (this) {
                notifyAll();
            }
            Thread.currentThread().setPriority(previousPriority);
        } else {
            if (wait) {
                synchronized (this) {
                    if (!exhausted) {
                        wait(5_000);
                    }
                }
            }
        }
        return counter;
    }

    private synchronized AbstractDataType<?>[] publishNextRow() throws SQLException, FunctionException {

        AbstractDataType<?>[] result = null;
        if (!isResultSetClosedQuietly() && resultSet.next()) {
            result = new AbstractDataType[rowMetadata.getColumnCount()];
            for (final ColumnMetadata columnMetadata : rowMetadata.getColumnsMetadata()) {
                result[columnMetadata.getIndex() - 1] = columnMetadata.getFunction().get(resultSet);
            }
        }
        return result;
    }

    private boolean isResultSetClosedQuietly() {
        boolean result = false;
        try {
            result = resultSet.isClosed();
        } catch (final Throwable t) { //NOPMD
            // ignore
        }
        return result;
    }
}
