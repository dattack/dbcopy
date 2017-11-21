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

import java.sql.SQLException;

/**
 * @author cvarela
 * @since 0.1
 */
public final class DbCopyTaskResult implements DbCopyTaskResultMBean {

    private final String taskName;
    private long startTime;
    private long endTime;
    private int retrievedRows;
    private int insertedRows;
    private SQLException exception;

    public DbCopyTaskResult(final String taskName) {
        this.taskName = taskName;
        this.retrievedRows = 0;
        this.insertedRows = 0;
        this.startTime = 0;
        this.endTime = 0;
        this.exception = null;
    }

    public void addInsertedRows(final int value) {
        this.insertedRows += value;
    }

    public void end() {
        this.endTime = System.currentTimeMillis();
    }

    @Override
    public long getEndTime() {
        return endTime;
    }

    @Override
    public SQLException getException() {
        return exception;
    }

    public long getExecutionTime() {
        if (startTime <= 0) {
            return 0;
        }

        if (endTime <= 0) {
            return System.currentTimeMillis() - startTime;
        }
        return endTime - startTime;
    }

    @Override
    public int getInsertedRows() {
        return insertedRows;
    }

    @Override
    public float getRateRowsPerSecond() {

        final long executionTime = getExecutionTime();
        if (executionTime <= 0) {
            return 0;
        }

        return getInsertedRows() * 1000F / executionTime;
    }

    @Override
    public int getRetrievedRows() {
        return retrievedRows;
    }

    @Override
    public long getStartTime() {
        return startTime;
    }

    @Override
    public String getTaskName() {
        return taskName;
    }

    public void incrementRetrievedRows() {
        this.retrievedRows += 1;
    }

    public void setException(final SQLException exception) {
        this.exception = exception;
    }

    public void start() {
        this.startTime = System.currentTimeMillis();
    }

    @Override
    public String toString() {

        final StringBuilder str = new StringBuilder().append("DbCopyTaskResult [taskName=").append(taskName)
                .append(", retrievedRows=").append(retrievedRows) //
                .append(", insertedRows=").append(insertedRows);

        if (exception != null) {
            str.append(", exception=").append(exception);
        }
        str.append(']');
        return str.toString();
    }

}
