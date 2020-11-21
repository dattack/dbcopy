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
import com.dattack.dbcopy.engine.*;
import com.dattack.dbcopy.engine.export.ExportOperation;
import com.dattack.dbcopy.engine.export.ExportOperationFactory;
import com.dattack.formats.csv.CSVConfiguration;
import com.dattack.formats.csv.CSVStringBuilder;
import com.dattack.jtoolbox.io.IOUtils;
import org.apache.commons.configuration.AbstractConfiguration;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang.exception.NestableRuntimeException;

import java.io.FileInputStream;
import java.io.IOException;
import java.util.Properties;

/**
 * @author cvarela
 * @since 0.1
 */
public class CsvExportOperationFactory implements ExportOperationFactory {

    private final ExportOperationBean bean;
    private final AbstractConfiguration configuration;
    private CsvExportWriteWrapper writer;

    public CsvExportOperationFactory(final ExportOperationBean bean, final AbstractConfiguration configuration) {
        this.bean = bean;
        this.configuration = configuration;
    }

    private synchronized void initWriter(RowMetadata rowMetadata) throws IOException {
        if (writer == null) {
            writer = new CsvExportWriteWrapper(bean, configuration);
            writer.setHeader(getHeader(rowMetadata));
        }
    }

    private String getHeader(RowMetadata rowMetadata) throws IOException {

        CSVStringBuilder builder = createCSVStringBuilder();
        for (ColumnMetadata columnMetadata: rowMetadata.getColumnsMetadata()) {
            builder.append(columnMetadata.getName());
        }
        builder.eol();
        return builder.toString();
    }

    private CSVStringBuilder createCSVStringBuilder() throws IOException {

        Properties properties = new Properties();
        if (StringUtils.isNotBlank(bean.getFormatFile())) {
            properties.load(new FileInputStream(bean.getFormatFile()));
        }
        CSVConfiguration configuration = new CSVConfiguration.CsvConfigurationBuilder(properties)
                .build();
        return new CSVStringBuilder(configuration);
    }

    @Override
    public ExportOperation createTask(DataTransfer dataTransfer, DbCopyTaskResult taskResult) {

        try {
            initWriter(dataTransfer.getRowMetadata());

            taskResult.addOnEndCommand(() -> {
                        IOUtils.closeQuietly(writer);
                        return null;
                    }
            );

            return new CsvExportOperation(bean, dataTransfer, taskResult, writer);

        } catch (IOException e) {
            throw new NestableRuntimeException(e);
        }
    }
}