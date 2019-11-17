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

import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

import org.apache.commons.configuration.AbstractConfiguration;
import org.apache.commons.configuration.BaseConfiguration;
import org.apache.commons.configuration.CompositeConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.dattack.dbcopy.beans.AbstractVariableBean;
import com.dattack.dbcopy.beans.DbcopyJobBean;
import com.dattack.dbcopy.beans.LiteralListBean;
import com.dattack.dbcopy.beans.IntegerRangeBean;
import com.dattack.dbcopy.beans.NullVariableBean;
import com.dattack.dbcopy.beans.VariableVisitor;
import com.dattack.jtoolbox.commons.configuration.ConfigurationUtil;

/**
 * Executes a copy-job instance.
 *
 * @author cvarela
 * @since 0.1
 */
class DbCopyJob {

    private static final Logger LOGGER = LoggerFactory.getLogger(DbCopyJob.class);

    private final DbcopyJobBean dbcopyJobBean;
    private final ExecutionController executionController;
    private final AbstractConfiguration externalConfiguration;

    private static void show(final DbCopyJobResult jobResult) {

        final StringBuilder sb = new StringBuilder();
        sb.append("\nJob ID: ").append(jobResult.getJobBean().getId());
        sb.append("\nSelect statement: ").append(jobResult.getJobBean().getSelectBean().getSql());
        sb.append("\nNumber of tasks: ").append(jobResult.getTotalTaskCounter());
        sb.append("\nNumber of finished tasks: ").append(jobResult.getFinishedTaskCounter());

        final SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSSZ");

        for (final DbCopyTaskResult taskResult : jobResult.getTaskResultList()) {
            sb.append("\n\tTask name: ").append(taskResult.getTaskName());
            sb.append("\n\t\tStart time: ").append(sdf.format(new Date(taskResult.getStartTime())));
            sb.append("\n\t\tEnd time: ").append(sdf.format(new Date(taskResult.getEndTime())));
            sb.append("\n\t\tExecution time: ").append(taskResult.getExecutionTime()).append(" ms.");
            sb.append("\n\t\tRetrieved rows: ").append(taskResult.getRetrievedRows());
            sb.append("\n\t\tInserted rows: ").append(taskResult.getInsertedRows());
            sb.append("\n\t\tRate read(rows/s): ").append(taskResult.getRateRowsRetrievedPerSecond());
            sb.append("\n\t\tRate write(rows/s): ").append(taskResult.getRateRowsInsertedPerSecond());
            if (taskResult.getException() != null) {
                sb.append("\n\t\tException: ").append(taskResult.getException().getMessage());
            }
        }

        LOGGER.info(sb.toString());
    }

    private static void showFutures(final List<Future<?>> futureList) {

        for (final Future<?> future : futureList) {
            try {
                LOGGER.info("Future result: {}", future.get());
            } catch (final InterruptedException | ExecutionException e) {
                LOGGER.warn("Error getting computed result from Future object", e);
            }
        }
    }

    DbCopyJob(final DbcopyJobBean dbcopyJobBean, final AbstractConfiguration configuration) {
        this.dbcopyJobBean = dbcopyJobBean;
        this.externalConfiguration = configuration;
        executionController = new ExecutionController(dbcopyJobBean.getId(), dbcopyJobBean.getThreads(),
                dbcopyJobBean.getThreads());
        MBeanHelper.registerMBean("com.dattack.dbcopy:type=ThreadPool,name=" + dbcopyJobBean.getId(),
                executionController);
    }

    void execute() {

        LOGGER.info("Running job '{}' at thread '{}'", dbcopyJobBean.getId(), Thread.currentThread().getName());

        final List<Future<?>> futureList = new ArrayList<>();

        final DbCopyJobResult jobResult = new DbCopyJobResult(dbcopyJobBean);
        MBeanHelper.registerMBean("com.dattack.dbcopy:type=JobResult,name=" + dbcopyJobBean.getId(), jobResult);

        final VariableVisitor rangeVisitor = new VariableVisitor() {

            private BaseConfiguration createBaseConfiguration() {
                BaseConfiguration baseConfiguration = new BaseConfiguration();
                baseConfiguration.setDelimiterParsingDisabled(true);
                baseConfiguration.setProperty("job.id", dbcopyJobBean.getId());
                return baseConfiguration;
            }

            @Override
            public void visite(LiteralListBean bean) {

                Iterator<Integer> it = bean.getValues().iterator();

                int taskId = 0;
                while (it.hasNext()) {

                    List<Integer> values = new ArrayList<>();
                    while (it.hasNext() && values.size() < bean.getBlockSize()) {
                        values.add(it.next());
                    }

                    final BaseConfiguration baseConfiguration = createBaseConfiguration();
                    baseConfiguration.setProperty(bean.getId() + ".values", values);

                    final CompositeConfiguration configuration = new CompositeConfiguration();
                    configuration.addConfiguration(externalConfiguration);
                    configuration.addConfiguration(ConfigurationUtil.createEnvSystemConfiguration());
                    configuration.addConfiguration(baseConfiguration);

                    final String taskName = String.format("Task_%d", taskId++);

                    final DbCopyTask dbcopyTask = new DbCopyTask(dbcopyJobBean, configuration,
                            jobResult.createTaskResult(taskName));
                    futureList.add(executionController.submit(dbcopyTask));
                }

            }

            @Override
            public void visite(final IntegerRangeBean bean) {

                for (long i = bean.getLowValue(); i < bean.getHighValue(); i += bean.getBlockSize()) {

                    final long lowValue = i;
                    final long highValue = i + bean.getBlockSize();

                    final BaseConfiguration baseConfiguration = createBaseConfiguration();
                    baseConfiguration.setProperty(bean.getId() + ".low", lowValue);
                    baseConfiguration.setProperty(bean.getId() + ".high", highValue);

                    final CompositeConfiguration configuration = new CompositeConfiguration();
                    configuration.addConfiguration(externalConfiguration);
                    configuration.addConfiguration(ConfigurationUtil.createEnvSystemConfiguration());
                    configuration.addConfiguration(baseConfiguration);

                    final String taskName = String.format("Task_%d_%d", lowValue, highValue);

                    final DbCopyTask dbcopyTask = new DbCopyTask(dbcopyJobBean, configuration,
                            jobResult.createTaskResult(taskName));
                    futureList.add(executionController.submit(dbcopyTask));
                }
            }

            @Override
            public void visite(final NullVariableBean bean) {

                final String taskName = "SingleTask";
                final CompositeConfiguration configuration = new CompositeConfiguration();
                configuration.addConfiguration(externalConfiguration);
                configuration.addConfiguration(ConfigurationUtil.createEnvSystemConfiguration());
                configuration.addConfiguration(createBaseConfiguration());

                final DbCopyTask dbcopyTask = new DbCopyTask(dbcopyJobBean, configuration,
                        jobResult.createTaskResult(taskName));
                futureList.add(executionController.submit(dbcopyTask));
            }
        };

        if (dbcopyJobBean.getVariableList() == null || dbcopyJobBean.getVariableList().isEmpty()) {
            new NullVariableBean().accept(rangeVisitor);
        } else {
            for (final AbstractVariableBean item : dbcopyJobBean.getVariableList()) {
                item.accept(rangeVisitor);
            }
        }

        executionController.shutdown();
        showFutures(futureList);

        show(jobResult);

        LOGGER.info("Job finished (job-name: '{}', thread: '{}')", dbcopyJobBean.getId(),
                Thread.currentThread().getName());
    }
}
