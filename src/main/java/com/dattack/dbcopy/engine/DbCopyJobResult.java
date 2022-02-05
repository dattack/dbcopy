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

import com.dattack.dbcopy.beans.DbcopyJobBean;
import java.util.ArrayList;
import java.util.List;

/**
 * MBean implementation to access the result of the execution of a job.
 *
 * @author cvarela
 * @since 0.1
 */
public class DbCopyJobResult implements DbCopyJobResultMBean {

    private final DbcopyJobBean jobBean;
    private final List<DbCopyTaskResult> taskResultList;

    public DbCopyJobResult(final DbcopyJobBean jobBean) {
        this.jobBean = jobBean;
        this.taskResultList = new ArrayList<>();
        MBeanHelper.registerMBean("JobResult", jobBean.getId(), this);
    }

    public DbCopyTaskResult createTaskResult(final String taskName) {
        final DbCopyTaskResult taskResult = new DbCopyTaskResult(taskName);
        this.taskResultList.add(taskResult);
        return taskResult;
    }

    @Override
    public int getActiveTaskCounter() {
        int counter = 0;
        for (final DbCopyTaskResult item : taskResultList) {
            if (item.getStartTime() > 0 && item.getEndTime() <= 0) {
                counter++;
            }
        }
        return counter;
    }

    @Override
    public int getFinishedTaskCounter() {
        int counter = 0;
        for (final DbCopyTaskResult item : taskResultList) {
            if (item.getEndTime() > 0) {
                counter++;
            }
        }
        return counter;
    }

    @Override
    public int getInactiveTaskCounter() {
        int counter = 0;
        for (final DbCopyTaskResult item : taskResultList) {
            if (item.getStartTime() <= 0) {
                counter++;
            }
        }
        return counter;
    }

    @Override
    public float getProcessedRowsPerSecond() {
        float rate = 0F;
        for (final DbCopyTaskResult item : taskResultList) {
            if (item.getStartTime() > 0 && item.getEndTime() <= 0) {
                rate += item.getProcessedRowsPerSecond();
            }
        }
        return rate;
    }

    @Override
    public float getRetrievedRowsPerSecond() {
        float rate = 0F;
        for (final DbCopyTaskResult item : taskResultList) {
            if (item.getStartTime() > 0 && item.getEndTime() <= 0) {
                rate += item.getRetrievedRowsPerSecond();
            }
        }
        return rate;
    }

    @Override
    public int getTotalTaskCounter() {
        return taskResultList.size();
    }

    @Override
    public long getTotalRetrievedRows() {
        long total = 0;
        for (final DbCopyTaskResult item : taskResultList) {
            total += item.getTotalRetrievedRows();
        }
        return total;
    }

    @Override
    public long getTotalProcessedRows() {
        long total = 0;
        for (final DbCopyTaskResult item : taskResultList) {
            total += item.getTotalProcessedRows();
        }
        return total;
    }

    public DbcopyJobBean getJobBean() {
        return jobBean;
    }

    public List<DbCopyTaskResult> getTaskResultList() {
        return taskResultList;
    }
}
