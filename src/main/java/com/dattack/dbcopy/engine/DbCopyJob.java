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

import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import org.apache.commons.configuration.AbstractConfiguration;
import org.apache.commons.configuration.BaseConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.dattack.dbcopy.beans.DbcopyTaskBean;
import com.dattack.dbcopy.beans.IntegerRangeBean;
import com.dattack.dbcopy.beans.RangeBean;
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

    public DbCopyJob(final DbcopyTaskBean dbcopyTaskBean) {
        this.dbcopyTaskBean = dbcopyTaskBean;
        this.threadPoolExecutor = new ThreadPoolExecutor(dbcopyTaskBean.getThreads(), dbcopyTaskBean.getThreads(), 1L,
                TimeUnit.MINUTES, new LinkedBlockingQueue<Runnable>());
    }

    public void execute() {

        final String threadName = Thread.currentThread().getName();

        LOGGER.info("Running job '{}' at thread '{}'", dbcopyTaskBean.getId(), threadName);

        final RangeVisitor rangeVisitor = new RangeVisitor() {

            @Override
            public void visite(final IntegerRangeBean bean) {

                for (int i = bean.getLowValue(); i < bean.getHighValue(); i += bean.getBlockSize()) {
                    final AbstractConfiguration configuration = new BaseConfiguration();
                    configuration.setProperty(bean.getId() + ".low", i);
                    configuration.setProperty(bean.getId() + ".high", i + bean.getBlockSize());
                    final DbCopyTask dbcopyTask = new DbCopyTask(dbcopyTaskBean, configuration);

                    threadPoolExecutor.submit(dbcopyTask);
                }
            }
        };

        final RangeBean rangeBean = dbcopyTaskBean.getRangeBean();
        rangeBean.accept(rangeVisitor);

        threadPoolExecutor.shutdown();

        while (!threadPoolExecutor.isTerminated()) {
            try {
                threadPoolExecutor.awaitTermination(30, TimeUnit.SECONDS);
            } catch (final InterruptedException e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
            }
        }

        LOGGER.info("Job finished (job-name: '{}', thread: '{}')", dbcopyTaskBean.getId(), threadName);
    }
}
