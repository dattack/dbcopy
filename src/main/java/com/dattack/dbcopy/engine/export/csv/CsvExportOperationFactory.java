/*
 * Copyright (c) 2020, The Dattack team (http://www.dattack.com)
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
package com.dattack.dbcopy.engine.export.csv;

import com.dattack.dbcopy.beans.ExportOperationBean;
import com.dattack.dbcopy.engine.ColumnMetadata;
import com.dattack.dbcopy.engine.DataTransfer;
import com.dattack.dbcopy.engine.DbCopyTaskResult;
import com.dattack.dbcopy.engine.RowMetadata;
import com.dattack.dbcopy.engine.export.ExportOperation;
import com.dattack.dbcopy.engine.export.ExportOperationFactory;
import com.dattack.formats.csv.CSVConfiguration;
import com.dattack.formats.csv.CSVStringBuilder;
import com.dattack.jtoolbox.io.IOUtils;
import org.apache.commons.configuration.AbstractConfiguration;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang.exception.NestableRuntimeException;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Objects;
import java.util.Properties;

/**
 * Factory responsible for the instantiation of export operations in CSV format.
 *
 * @author cvarela
 * @since 0.1
 */
public class CsvExportOperationFactory implements ExportOperationFactory {

    private final transient ExportOperationBean bean;
    private final transient AbstractConfiguration configuration;
    private transient CsvExportWriteWrapper writer;

    public CsvExportOperationFactory(final ExportOperationBean bean, final AbstractConfiguration configuration) {
        this.bean = bean;
        this.configuration = configuration;
    }

    private synchronized CsvExportWriteWrapper getWriter(final RowMetadata rowMetadata) throws IOException {
        if (Objects.isNull(writer)) {
            writer = new CsvExportWriteWrapper(bean, configuration);
            writer.setHeader(getHeader(rowMetadata));
        }
        return writer;
    }

    private String getHeader(final RowMetadata rowMetadata) throws IOException {

        final CSVStringBuilder builder = createCSVStringBuilder();
        for (final ColumnMetadata columnMetadata : rowMetadata.getColumnsMetadata()) {
            builder.append(columnMetadata.getName());
        }
        builder.eol();
        return builder.toString();
    }

    @SuppressWarnings("checkstyle:AbbreviationAsWordInName")
    private CSVStringBuilder createCSVStringBuilder() throws IOException {

        final Properties properties = new Properties();
        if (StringUtils.isNotBlank(bean.getFormatFile())) {
            try (InputStream fis = Files.newInputStream(Paths.get(bean.getFormatFile()))) {
                properties.load(fis);
            }
        }
        final CSVConfiguration configuration = CSVConfiguration.custom(properties).build();
        return new CSVStringBuilder(configuration);
    }

    @Override
    public ExportOperation createTask(final DataTransfer dataTransfer, final DbCopyTaskResult taskResult) {

        try {
            final CsvExportWriteWrapper outputWriter = getWriter(dataTransfer.getRowMetadata()); //NOPMD: resource
            // can't be closed here

            taskResult.addOnEndCommand(() -> {
                    IOUtils.closeQuietly(outputWriter);
                    return null;
                }
            );

            return new CsvExportOperation(bean, dataTransfer, taskResult, outputWriter);

        } catch (IOException e) {
            throw new NestableRuntimeException(e);
        }
    }
}
