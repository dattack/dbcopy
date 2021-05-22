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

import java.io.File;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

import org.apache.commons.configuration.AbstractConfiguration;

import com.dattack.dbcopy.beans.DbcopyBean;
import com.dattack.dbcopy.beans.DbcopyJobBean;
import com.dattack.dbcopy.beans.DbcopyParser;
import com.dattack.jtoolbox.exceptions.DattackParserException;
import com.dattack.jtoolbox.io.FilesystemUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Responsible of the execution of one or more jobs.
 *
 * @author cvarela
 * @since 0.1
 */
public final class DbCopyEngine {

    private static final Logger LOGGER = LoggerFactory.getLogger(DbCopyEngine.class);

    private void execute(final DbcopyBean dbcopyBean, final Set<String> jobNames,
                         final AbstractConfiguration configuration) {

        ExecutionController executionController = new ExecutionController("root", dbcopyBean.getParallel(),
                dbcopyBean.getParallel());
        MBeanHelper.registerMBean("com.dattack.dbcopy:type=ThreadPool,name=root",
                executionController);

        final List<Future<?>> futureList = new ArrayList<>();
        for (final DbcopyJobBean jobBean : dbcopyBean.getJobList()) {

            if (jobNames != null && !jobNames.isEmpty() && !jobNames.contains(jobBean.getId())) {
                continue;
            }

            final DbCopyJob job = new DbCopyJob(jobBean, configuration);
            futureList.add(executionController.submit(job));
        }

        executionController.shutdown();

        for (final Future<?> future : futureList) {
            try {
                LOGGER.info("Future result: {}", future.get());
            } catch (final InterruptedException | ExecutionException e) {
                LOGGER.warn("Error getting computed result from Future object", e);
            }
        }
    }

    private void execute(final File file, final Set<String> jobNames, final AbstractConfiguration configuration)
            throws DattackParserException {

        if (file.isDirectory()) {

            final File[] files = file.listFiles(FilesystemUtils.createFilenameFilterByExtension("xml"));
            if (files != null) {
                for (final File child : files) {
                    execute(child, jobNames, configuration);
                }
            }

        } else {

            final DbcopyBean dbcopyBean = DbcopyParser.parse(file);
            execute(dbcopyBean, jobNames, configuration);
        }
    }

    public void execute(final String[] filenames, final Set<String> jobNames, final AbstractConfiguration configuration)
            throws DattackParserException {

        for (final String filename : filenames) {
            execute(new File(filename), jobNames, configuration);
        }
    }
}
