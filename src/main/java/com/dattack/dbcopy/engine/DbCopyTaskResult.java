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
import java.util.UUID;
import java.util.concurrent.atomic.AtomicLong;

/**
 * MBean implementation to access the result of the execution of a task.
 *
 * @author cvarela
 * @since 0.1
 */
public final class DbCopyTaskResult implements DbCopyTaskResultMBean {

    private final transient List<Command<?>> onEndCommandList;
    private final transient AtomicLong processedRows;
    private final transient AtomicLong retrievedRows;
    private final transient String taskName;
    private transient long endTime;
    private transient Exception exception;
    private transient long startTime;
    private transient String executionId;

    public DbCopyTaskResult(final String taskName) {
        this.taskName = taskName;
        this.retrievedRows = new AtomicLong(0);
        this.processedRows = new AtomicLong(0);
        this.startTime = 0;
        this.endTime = 0;
        //this.exception = null;
        this.onEndCommandList = new ArrayList<>();
        MBeanHelper.registerMBean("TaskResult", taskName, this);
    }

    public void addOnEndCommand(final Command<?> command) {
        onEndCommandList.add(command);
    }

    public void addProcessedRows(final int value) {
        this.processedRows.addAndGet(value);
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

    @Override
    public float getProcessedRowsPerSecond() {

        float result = 0;
        final long executionTime = getExecutionTime();
        if (executionTime > 0) {
            result = getTotalProcessedRows() * 1000F / executionTime;
        }
        return result;
    }

    @Override
    public float getRetrievedRowsPerSecond() {

        float result = 0;
        final long executionTime = getExecutionTime();
        if (executionTime > 0) {
            result = getTotalRetrievedRows() * 1000F / executionTime;
        }
        return result;
    }

    @Override
    public long getStartTime() {
        return startTime;
    }

    @Override
    public String getTaskName() {
        return taskName;
    }

    @Override
    public long getTotalProcessedRows() {
        return processedRows.longValue();
    }

    @Override
    public long getTotalRetrievedRows() {
        return retrievedRows.longValue();
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

    public void incrementRetrievedRows() {
        this.retrievedRows.incrementAndGet();
    }

    public void start() {
        this.startTime = System.currentTimeMillis();
        this.executionId = UUID.randomUUID().toString();
    }

    public String getExecutionId() {
        return executionId;
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
