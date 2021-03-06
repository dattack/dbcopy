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

import java.util.ArrayList;
import java.util.List;

import com.dattack.dbcopy.beans.DbcopyJobBean;

/**
 * @author cvarela
 * @since 0.1
 */
public class DbCopyJobResult implements DbCopyJobResultMBean {

    private final DbcopyJobBean jobBean;
    private final List<DbCopyTaskResult> taskResultList;

    public DbCopyJobResult(final DbcopyJobBean jobBean) {
        this.jobBean = jobBean;
        this.taskResultList = new ArrayList<>();
    }

    public DbCopyTaskResult createTaskResult(final String taskName) {

        final String fullTaskName = String.format("%s@%s", jobBean.getId(), taskName);
        final DbCopyTaskResult taskResult = new DbCopyTaskResult(fullTaskName);
        this.taskResultList.add(taskResult);
        MBeanHelper.registerMBean("com.dattack.dbcopy:type=TaskResult,name=" + fullTaskName, taskResult);
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

    public DbcopyJobBean getJobBean() {
        return jobBean;
    }

    public List<DbCopyTaskResult> getTaskResultList() {
        return taskResultList;
    }

    @Override
    public int getTotalTaskCounter() {
        return taskResultList.size();
    }

    @Override
    public long getTotalRetrievedRows() {
        long total = 0;
        for (final DbCopyTaskResult item : taskResultList) {
            total += item.getRetrievedRows();
        }
        return total;
    }

    @Override
    public long getTotalInsertedRows() {
        long total = 0;
        for (final DbCopyTaskResult item : taskResultList) {
            total += item.getInsertedRows();
        }
        return total;
    }

    @Override
    public float getRateRowsInsertedPerSecond() {
        float rate = 0F;
        for (final DbCopyTaskResult item : taskResultList) {
            if (item.getStartTime() > 0 && item.getEndTime() <= 0) {
                rate += item.getRateRowsInsertedPerSecond();
            }
        }
        return rate;
    }

    @Override
    public float getRateRowsRetrievedPerSecond() {
        float rate = 0F;
        for (final DbCopyTaskResult item : taskResultList) {
            if (item.getStartTime() > 0 && item.getEndTime() <= 0) {
                rate += item.getRateRowsRetrievedPerSecond();
            }
        }
        return rate;
    }
}
