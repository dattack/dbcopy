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
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import org.apache.commons.configuration.AbstractConfiguration;
import org.apache.commons.configuration.BaseConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.dattack.dbcopy.beans.DbcopyTaskBean;
import com.dattack.dbcopy.beans.IntegerRangeBean;
import com.dattack.dbcopy.beans.RangeVisitor;

/**
 * Executes a copy-job instance.
 *
 * @author cvarela
 * @since 0.1
 */
class DbCopyJob {

    private static final Logger LOGGER = LoggerFactory.getLogger(DbCopyJob.class);

    private final DbcopyTaskBean dbcopyTaskBean;
    private final ThreadPoolExecutor threadPoolExecutor;

    private static void showFutures(final List<Future<?>> futureList) {

        for (final Future<?> future : futureList) {
            try {
                LOGGER.info("Future result: {}", future.get());
            } catch (final InterruptedException | ExecutionException e) {
                LOGGER.warn("Error getting computed result from Future object", e);
            }
        }
    }
    public DbCopyJob(final DbcopyTaskBean dbcopyTaskBean) {
        this.dbcopyTaskBean = dbcopyTaskBean;
        this.threadPoolExecutor = new ThreadPoolExecutor(dbcopyTaskBean.getThreads(), dbcopyTaskBean.getThreads(), 1L,
                TimeUnit.MINUTES, new LinkedBlockingQueue<Runnable>());
    }

    public void execute() {

        LOGGER.info("Running job '{}' at thread '{}'", dbcopyTaskBean.getId(), Thread.currentThread().getName());

        final List<Future<?>> futureList = new ArrayList<>();

        final RangeVisitor rangeVisitor = new RangeVisitor() {

            @Override
            public void visite(final IntegerRangeBean bean) {

                for (int i = bean.getLowValue(); i < bean.getHighValue(); i += bean.getBlockSize()) {
                    final IntegerRangeValue range = new IntegerRangeValue(i, i + bean.getBlockSize());
                    final AbstractConfiguration configuration = new BaseConfiguration();
                    configuration.setProperty(bean.getId() + ".low", range.getLowValue());
                    configuration.setProperty(bean.getId() + ".high", range.getHighValue());

                    final DbCopyTask dbcopyTask = new DbCopyTask(dbcopyTaskBean, configuration, range);
                    futureList.add(threadPoolExecutor.submit(dbcopyTask));
                }
            }
        };

        dbcopyTaskBean.getRangeBean().accept(rangeVisitor);

        threadPoolExecutor.shutdown();
        showFutures(futureList);

        LOGGER.info("Job finished (job-name: '{}', thread: '{}')", dbcopyTaskBean.getId(),
                Thread.currentThread().getName());
    }
}
