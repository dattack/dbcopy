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

import com.dattack.jtoolbox.patterns.Command;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

/**
 * MBean implementation to access the result of the execution of a task.
 *
 * @author cvarela
 * @since 0.1
 */
public final class DbCopyTaskResult implements DbCopyTaskResultMBean {

    private final String taskName;
    private long startTime;
    private long endTime;
    private final AtomicLong retrievedRows;
    private final AtomicLong processedRows;
    private Exception exception;
    private final List<Command<?>> onEndCommandList;

    public DbCopyTaskResult(final String taskName) {
        this.taskName = taskName;
        this.retrievedRows = new AtomicLong(0);
        this.processedRows = new AtomicLong(0);
        this.startTime = 0;
        this.endTime = 0;
        this.exception = null;
        this.onEndCommandList = new ArrayList<>();
    }

    public void addProcessedRows(final int value) {
        this.processedRows.addAndGet(value);
    }

    public void addOnEndCommand(Command<?> command) {
        onEndCommandList.add(command);
    }

    public void end() {
        this.endTime = System.currentTimeMillis();
        onEndCommandList.forEach(Command::execute);
    }

    @Override
    public long getEndTime() {
        return endTime;
    }

    @Override
    public Exception getException() {
        return exception;
    }

    public void setException(final Exception exception) {
        this.exception = exception;
    }

    public long getExecutionTime() {
        long result = 0;
        if (startTime > 0) {
            if (endTime <= 0) {
                result = System.currentTimeMillis() - startTime;
            } else {
                result = endTime - startTime;
            }
        }
        return result;
    }

    @Override
    public long getTotalProcessedRows() {
        return processedRows.longValue();
    }

    @Override
    public float getProcessedRowsPerSecond() {

        final long executionTime = getExecutionTime();
        if (executionTime <= 0) {
            return 0;
        }

        return getTotalProcessedRows() * 1000F / executionTime;
    }

    @Override
    public float getRetrievedRowsPerSecond() {

        final long executionTime = getExecutionTime();
        if (executionTime <= 0) {
            return 0;
        }

        return getTotalRetrievedRows() * 1000F / executionTime;
    }

    @Override
    public long getTotalRetrievedRows() {
        return retrievedRows.longValue();
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
        this.retrievedRows.incrementAndGet();
    }

    public void start() {
        this.startTime = System.currentTimeMillis();
    }

    @Override
    public String toString() {

        final StringBuilder str = new StringBuilder().append("DbCopyTaskResult [taskName=").append(taskName)
                .append(", retrievedRows=").append(retrievedRows) //
                .append(", processedRows=").append(processedRows);

        if (exception != null) {
            str.append(", exception=").append(exception);
        }
        str.append(']');
        return str.toString();
    }

}
