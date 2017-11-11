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
import java.util.Set;

import org.apache.commons.configuration.ConfigurationException;

import com.dattack.dbcopy.beans.DbcopyBean;
import com.dattack.dbcopy.beans.DbcopyParser;
import com.dattack.dbcopy.beans.DbcopyTaskBean;
import com.dattack.jtoolbox.exceptions.DattackParserException;
import com.dattack.jtoolbox.io.FilesystemUtils;

/**
 * @author cvarela
 * @since 0.1
 */
public final class DbCopyEngine {

    private void execute(final File file, final Set<String> taskNames)
            throws ConfigurationException, DattackParserException {

        if (file.isDirectory()) {

            final File[] files = file.listFiles(FilesystemUtils.createFilenameFilterByExtension("xml"));
            if (files != null) {
                for (final File child : files) {
                    execute(child, taskNames);
                }
            }

        } else {

            final DbcopyBean dbcopyBean = DbcopyParser.parse(file);

            for (final DbcopyTaskBean copyTaskBean : dbcopyBean.getTaskList()) {

                if (taskNames != null && !taskNames.isEmpty() && !taskNames.contains(copyTaskBean.getId())) {
                    continue;
                }

                final DbCopyJob job = new DbCopyJob(copyTaskBean);
                job.execute();
            }
        }
    }

    public void execute(final String[] filenames, final Set<String> taskNames)
            throws ConfigurationException, DattackParserException {

        for (final String filename : filenames) {
            execute(new File(filename), taskNames);
        }
    }
}
