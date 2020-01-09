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
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.sql.*;
import java.util.Iterator;
import java.util.List;
import java.util.Properties;
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
    private final DataTransfer dataTransfer;
    private DbCopyTaskResult taskResult;
    private ExportWriteWrapper writer;

    ExportOperation(final ExportOperationBean bean, final DataTransfer dataTransfer,
                    final DbCopyTaskResult taskResult, final ExportWriteWrapper writer) {
        this.bean = bean;
        this.dataTransfer = dataTransfer;
        this.taskResult = taskResult;
        this.writer = writer;
    }

    public static ExportWriteWrapper createExportOutputWriter(ExportOperationBean exportOperationBean, AbstractConfiguration configuration) throws IOException {

        Path path = Paths.get(ConfigurationUtil.interpolate(exportOperationBean.getPath(), configuration));
        Path parent = path.getParent();
        if (parent != null) {
            Files.createDirectories(parent);
        }
        OutputStream outputStream =  Files.newOutputStream(path,
                StandardOpenOption.CREATE, //
                StandardOpenOption.WRITE, //
                StandardOpenOption.TRUNCATE_EXISTING);

        if (exportOperationBean.isGzip()) {
            outputStream = new GZIPOutputStream(outputStream);
        }

        return new ExportWriteWrapper(path, new BufferedWriter(new OutputStreamWriter(outputStream,
                StandardCharsets.UTF_8)));
    }

    private String getHeader(CSVStringBuilder builder) {

        for (ColumnMetadata columnMetadata: dataTransfer.getRowMetadata().getColumnsMetadata()) {
            builder.append(columnMetadata.getName());
        }
        builder.eol();
        return builder.toString();
    }

    public static CSVStringBuilder createCSVStringBuilder(final String formatFile) throws IOException {

        Properties properties = new Properties();
        if (StringUtils.isNotBlank(formatFile)) {
            properties.load(new FileInputStream(formatFile));
        }
        CSVConfiguration configuration = new CSVConfiguration.CsvConfigurationBuilder(properties)
                .build();
        return new CSVStringBuilder(configuration);
    }

    @Override
    public Integer call() throws SQLException, IOException, InterruptedException {

        int totalExportedRows = 0;
        try {

            CSVStringBuilder builder = createCSVStringBuilder(bean.getFormatFile());
            writer.open(getHeader(builder));
            builder.clear();

            while (true) {
                List<Object> row = dataTransfer.transfer();
                if (row == null) {
                    break;
                }
                populate(builder, row);
                taskResult.addProcessedRows(1);
                totalExportedRows++;
                if (totalExportedRows % bean.getBatchSize() == 0) {
                    String line = builder.toString();
                    writer.write(line);
                    builder.clear();
                }
            }
            String line = builder.toString();
            writer.write(line);
        } catch (IOException e) {
            LOGGER.error("I/O error {}: {}", writer, e.getMessage());
            throw e;
        }

        return totalExportedRows;
    }


    private void populate(CSVStringBuilder csvBuilder, List<Object> dataList) throws SQLException {

        Iterator<Object> dataIterator = dataList.iterator();

        for (ColumnMetadata columnMetadata: dataTransfer.getRowMetadata().getColumnsMetadata()) {
            final Object value = dataIterator.next();
            if (value == null) {
                csvBuilder.append((String) null);
            } else {
                switch (columnMetadata.getType()) {
                    case Types.CLOB:
                        if (value instanceof String) {
                            csvBuilder.append(value.toString());
                        } else {
                            Clob clob = (Clob) value;
                            csvBuilder.append(clob.getSubString(0L, (int) clob.length()));
                        }
                        break;
                    case Types.SQLXML:
                        SQLXML xml = (SQLXML) value;
                        csvBuilder.append(xml.getString());
                        break;
                    case Types.BOOLEAN:
                        Boolean b = (Boolean) value;
                        csvBuilder.append(b.toString());
                        break;
                    case Types.DATE:
                        csvBuilder.append((Date) value);
                        break;
                    case Types.TIME:
                    case Types.TIME_WITH_TIMEZONE:
                        csvBuilder.append((Time) value);
                        break;
                    case Types.TIMESTAMP:
                    case Types.TIMESTAMP_WITH_TIMEZONE:
                        csvBuilder.append((Timestamp) value);
                        break;
                    case Types.DECIMAL:
                        BigDecimal bigDecimal = (BigDecimal) value;
                        csvBuilder.append(bigDecimal.doubleValue());
                        break;
                    case Types.DOUBLE:
                        csvBuilder.append(((Number) value).doubleValue());
                        break;
                    case Types.REAL:
                    case Types.FLOAT:
                        csvBuilder.append(((Number) value).floatValue());
                        break;
                    case Types.TINYINT:
                    case Types.SMALLINT:
                    case Types.INTEGER:
                        csvBuilder.append(((Number) value).intValue());
                        break;
                    case Types.NUMERIC:
                        Number n = (Number) value;
                        int scale = columnMetadata.getScale();
                        if (scale == 0) {
                            csvBuilder.append(n.longValue());
                        } else {
                            csvBuilder.append(n.doubleValue());
                        }
                        break;
                    case Types.BIGINT:
                        Number bigInteger = (Number) value;
                        csvBuilder.append(bigInteger.longValue());
                        break;
                    case Types.BLOB:
                    case Types.CHAR:
                    case Types.VARCHAR:
                    default:
                        csvBuilder.append(value.toString());
                }
            }
        }
        csvBuilder.eol();
    }
}
