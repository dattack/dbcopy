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

import com.dattack.dbcopy.beans.AbstractVariableBean;
import com.dattack.dbcopy.beans.DbcopyJobBean;
import com.dattack.dbcopy.beans.IntegerRangeBean;
import com.dattack.dbcopy.beans.LiteralListBean;
import com.dattack.dbcopy.beans.NullVariableBean;
import com.dattack.dbcopy.beans.VariableVisitor;
import com.dattack.jtoolbox.commons.configuration.ConfigurationUtil;
import org.apache.commons.configuration.AbstractConfiguration;
import org.apache.commons.configuration.BaseConfiguration;
import org.apache.commons.configuration.CompositeConfiguration;
import org.apache.commons.io.FileUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.Iterator;
import java.util.List;
import java.util.Locale;
import java.util.Objects;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

import static java.lang.String.format;

/**
 * Executes a copy-job instance.
 *
 * @author cvarela
 * @since 0.1
 */
/* default */ class DbCopyJob implements Callable<Void> {

    private static final Logger LOGGER = LoggerFactory.getLogger(DbCopyJob.class);

    private final transient DbcopyJobBean dbcopyJobBean;
    private final transient AbstractConfiguration externalConfiguration;

    /* default */ DbCopyJob(final DbcopyJobBean dbcopyJobBean, final AbstractConfiguration configuration) {
        this.dbcopyJobBean = dbcopyJobBean;
        this.externalConfiguration = configuration;
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

    @Override
    public Void call() {

        LOGGER.info("Running job '{}' at thread '{}'", dbcopyJobBean.getId(), Thread.currentThread().getName());

        try (ExecutionController controller = new ExecutionController(dbcopyJobBean.getId(),
                                                                      dbcopyJobBean.getThreads())) {

            final List<Future<?>> futureList = new ArrayList<>();

            final DbCopyJobResult jobResult = new DbCopyJobResult(dbcopyJobBean);

            final VariableVisitor variableVisitor = getVariableVisitor(futureList, jobResult, controller);

            if (Objects.isNull(dbcopyJobBean.getVariableList()) || dbcopyJobBean.getVariableList().isEmpty()) {
                new NullVariableBean().accept(variableVisitor);
            } else {
                for (final AbstractVariableBean item : dbcopyJobBean.getVariableList()) {
                    item.accept(variableVisitor);
                }
            }

            controller.shutdown();
            showFutures(futureList);

            show(jobResult);
        }

        LOGGER.info("Job finished (job-name: '{}', thread: '{}')", dbcopyJobBean.getId(),
                    Thread.currentThread().getName());

        return null;
    }

    /* default */ DbcopyJobBean getDbcopyJobBean() {
        return dbcopyJobBean;
    }

    /* default */ AbstractConfiguration getExternalConfiguration() {
        return externalConfiguration;
    }

    private VariableVisitor getVariableVisitor(final List<Future<?>> futureList, final DbCopyJobResult jobResult,
        final ExecutionController executionController) {

        return new VariableVisitor() {

            @Override
            public void visit(final LiteralListBean bean) {

                final Iterator<String> iterator = bean.getValues().iterator();

                final StringBuilder taskName = new StringBuilder();

                while (iterator.hasNext()) {

                    taskName.append(getDbcopyJobBean().getId());

                    final List<String> values = new ArrayList<>(); //NOPMD
                    while (iterator.hasNext() && values.size() < bean.getBlockSize()) {
                        final String value = iterator.next();
                        taskName.append('_').append(value);
                        values.add(value);
                    }

                    final BaseConfiguration baseConfiguration = createBaseConfiguration();
                    baseConfiguration.setProperty(bean.getId() + ".values", values);
                    baseConfiguration.setProperty(bean.getId(), values);

                    final CompositeConfiguration configuration = createCompositeConfiguration();
                    configuration.addConfiguration(baseConfiguration);

                    final DbCopyTask dbcopyTask = new DbCopyTask(getDbcopyJobBean(), configuration, //NOPMD
                                                                 jobResult.createTaskResult(taskName.toString()));
                    futureList.add(executionController.submit(dbcopyTask));
                    taskName.setLength(0);
                }
            }

            @Override
            public void visit(final IntegerRangeBean bean) {

                for (long i = bean.getLowValue(); i < bean.getHighValue(); i += bean.getBlockSize()) {

                    final long highValue = Math.min(i + bean.getBlockSize(), bean.getHighValue());

                    final BaseConfiguration baseConfiguration = createBaseConfiguration();
                    baseConfiguration.setProperty(bean.getId() + ".low", i);
                    baseConfiguration.setProperty(bean.getId() + ".high", highValue);

                    final CompositeConfiguration configuration = createCompositeConfiguration();
                    configuration.addConfiguration(baseConfiguration);

                    final String taskName = String.format("%s_%d_%d", getDbcopyJobBean().getId(), i, highValue);

                    final DbCopyTask dbcopyTask = new DbCopyTask(getDbcopyJobBean(), configuration, //NOPMD
                                                                 jobResult.createTaskResult(taskName));
                    futureList.add(executionController.submit(dbcopyTask));
                }
            }

            @Override
            public void visit(final NullVariableBean bean) {

                final String taskName = getDbcopyJobBean().getId();
                final CompositeConfiguration configuration = createCompositeConfiguration();
                configuration.addConfiguration(createBaseConfiguration());

                final DbCopyTask dbcopyTask =
                    new DbCopyTask(getDbcopyJobBean(), configuration, jobResult.createTaskResult(taskName));
                futureList.add(executionController.submit(dbcopyTask));
            }

            private BaseConfiguration createBaseConfiguration() {
                final BaseConfiguration baseConfiguration = new BaseConfiguration();
                baseConfiguration.setDelimiterParsingDisabled(true);
                baseConfiguration.setProperty("job.id", getDbcopyJobBean().getId());
                return baseConfiguration;
            }

            private CompositeConfiguration createCompositeConfiguration() {
                final CompositeConfiguration configuration = new CompositeConfiguration();
                configuration.addConfiguration(getExternalConfiguration());
                configuration.addConfiguration(ConfigurationUtil.createEnvSystemConfiguration());
                return configuration;
            }
        };
    }

    private void show(final DbCopyJobResult jobResult) {

        final StringBuilder buffer = new StringBuilder(400);
        buffer.append("\nJob ID: ").append(jobResult.getJobBean().getId()) //
            .append("\nSelect statement: ").append(jobResult.getJobBean().getSelectBean().getSql()) //
            .append("\nNumber of tasks: ").append(jobResult.getTotalTaskCounter()) //
            .append("\nNumber of finished tasks: ").append(jobResult.getFinishedTaskCounter());

        final SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSSZ", Locale.getDefault());

        for (final DbCopyTaskResult taskResult : jobResult.getTaskResultList()) {
            buffer.append("\n\tTask name: ").append(taskResult.getTaskName()) //
                .append("\n\t\tStart time: ").append(sdf.format(new Date(taskResult.getStartTime()))) //
                .append("\n\t\tEnd time: ").append(sdf.format(new Date(taskResult.getEndTime()))) //
                .append("\n\t\tExecution time: ").append(String.format("%,d", taskResult.getExecutionTime())) //
                .append(" ms.\n\t\tRetrieved rows: ").append(format("%,d", taskResult.getTotalRetrievedRows())) //
                .append("\n\t\tProcessed rows: ").append(format("%,d", taskResult.getTotalProcessedRows())) //
                .append("\n\t\tRetrieved rows/s: ").append(format("%,f", taskResult.getRetrievedRowsPerSecond())) //
                .append("\n\t\tProcessed rows/s: ").append(format("%,f", taskResult.getProcessedRowsPerSecond()));

            if (taskResult.getException() != null) {
                buffer.append("\n\t\tException: ").append(taskResult.getException().getMessage());
            }

            if (taskResult.getTotalRetrievedRows() != taskResult.getTotalProcessedRows()) {
                buffer.append("\n\n\t\tJOB ENDED WITH ERRORS: SOME ROWS WERE NOT PROCESSED." //
                                  + "\n\t\tPLEASE CHECK THE LOG FILE FOR MORE DETAILS");
            }
        }

        try {
            FileUtils.writeStringToFile(new File(dbcopyJobBean.getId() + ".log"), buffer.toString());
        } catch (IOException e) {
            LOGGER.warn("Unable to write log file for job '{}'. {}", dbcopyJobBean.getId(), e);
        }

        LOGGER.info(buffer.toString());
    }
}
