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

import com.dattack.dbcopy.beans.ExportOperationBean;
import com.dattack.formats.csv.CSVConfiguration;
import com.dattack.formats.csv.CSVStringBuilder;
import com.dattack.jtoolbox.commons.configuration.ConfigurationUtil;
import org.apache.commons.configuration.AbstractConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.sql.SQLException;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.Callable;
import java.util.zip.GZIPOutputStream;

/**
 * Executes the EXPORT operations.
 *
 * @author cvarela
 * @since 0.1
 */
class ExportOperation implements Callable<Integer> {

    private final static Logger LOGGER = LoggerFactory.getLogger(ExportOperation.class);

    private final ExportOperationBean bean;
    private final DataProvider dataProvider;
    private DbCopyTaskResult taskResult;
    private Path path;

    ExportOperation(final ExportOperationBean bean, final DataProvider dataProvider,
                           final AbstractConfiguration configuration, DbCopyTaskResult taskResult) {
        this.bean = bean;
        this.dataProvider = dataProvider;
        this.taskResult = taskResult;
        this.path = Paths.get(ConfigurationUtil.interpolate(bean.getPath(), configuration));
    }

    private Writer createOutputWriter() throws IOException {

        OutputStream outputStream =  Files.newOutputStream(path,
                StandardOpenOption.CREATE, //
                StandardOpenOption.WRITE, //
                StandardOpenOption.TRUNCATE_EXISTING);

        if (bean.isGzip()) {
            outputStream = new GZIPOutputStream(outputStream);
        }

        return new BufferedWriter(new OutputStreamWriter(outputStream, StandardCharsets.UTF_8));
    }

    @Override
    public Integer call() throws SQLException, IOException {

        int totalExportedRows = 0;
        try (Writer writer = createOutputWriter()) {

            CSVConfiguration configuration = new CSVConfiguration.CsvConfigurationBuilder().build();
            CSVStringBuilder builder = new CSVStringBuilder(configuration);

            boolean header = true;
            while (dataProvider.populate(builder, header)) {
                writer.write(builder.toString());
                builder.clear();
                taskResult.addInsertedRows(1);
                totalExportedRows++;
                header = false;
                if (totalExportedRows % 10000 == 0) {
                    LOGGER.debug("Exported rows: {} (Current block: {})", 10000, totalExportedRows);
                }
            }
            writer.flush();
        }

        deleteEmptyFile();

        return totalExportedRows;
    }

    private void deleteEmptyFile() {
        try {
            if (Files.size(path) == 0) {
                Files.delete(path);
            }
        } catch (Exception e) {
            // ignore
        }
    }

    private String toCsv(List<Object> dataList) {

        CSVConfiguration configuration = new CSVConfiguration.CsvConfigurationBuilder().build();
        CSVStringBuilder builder = new CSVStringBuilder(configuration);
        for (Object obj: dataList) {
            builder.append(Objects.toString(obj));
        }
        builder.eol();
        return builder.toString();
    }
}
