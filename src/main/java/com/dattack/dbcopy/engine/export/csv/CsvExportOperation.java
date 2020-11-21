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
package com.dattack.dbcopy.engine.export.csv;

import com.dattack.dbcopy.beans.ExportOperationBean;
import com.dattack.dbcopy.engine.ColumnMetadata;
import com.dattack.dbcopy.engine.DataTransfer;
import com.dattack.dbcopy.engine.DbCopyTaskResult;
import com.dattack.dbcopy.engine.export.ExportOperation;
import com.dattack.formats.csv.CSVConfiguration;
import com.dattack.formats.csv.CSVStringBuilder;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileInputStream;
import java.io.IOException;
import java.math.BigDecimal;
import java.sql.*;
import java.util.Iterator;
import java.util.List;
import java.util.Properties;

/**
 * Executes the EXPORT operations.
 *
 * @author cvarela
 * @since 0.1
 */
public class CsvExportOperation implements ExportOperation {

    private final static Logger LOGGER = LoggerFactory.getLogger(CsvExportOperation.class);

    private final ExportOperationBean bean;
    private final DataTransfer dataTransfer;
    private DbCopyTaskResult taskResult;
    private final CsvExportWriteWrapper writer;

    CsvExportOperation(final ExportOperationBean bean, final DataTransfer dataTransfer,
                       final DbCopyTaskResult taskResult,
                       CsvExportWriteWrapper writer) {
        this.bean = bean;
        this.dataTransfer = dataTransfer;
        this.taskResult = taskResult;
        this.writer = writer;
    }

    private String getHeader(CSVStringBuilder builder) {

        for (ColumnMetadata columnMetadata: dataTransfer.getRowMetadata().getColumnsMetadata()) {
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
    public Integer call() throws SQLException, IOException, InterruptedException {

        int totalExportedRows = 0;

        try {
            CSVStringBuilder builder = createCSVStringBuilder();
            writer.setHeader(getHeader(builder));
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
                    LOGGER.debug("Exported rows: {}", totalExportedRows);
                    writer.write(builder.toString());
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
                    case Types.BIT:
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
                    case Types.NUMERIC:
                        BigDecimal bigDecimal = (BigDecimal) value;
                        int scale = bigDecimal.scale();
                         if (scale == 0) {
                             csvBuilder.append(bigDecimal.longValue());
                         } else {
                             csvBuilder.append(bigDecimal.doubleValue());
                         }
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
                    case Types.BIGINT:
                        Number bigInteger = (Number) value;
                        csvBuilder.append(bigInteger.longValue());
                        break;
                    // unsupported data types
                    case Types.BINARY:
                    case Types.VARBINARY:
                    case Types.LONGVARBINARY:
                        // byte[] -> base64
                    case Types.BLOB:
                    case Types.STRUCT:
                    case Types.REF:
                    case Types.ARRAY:
                    case Types.ROWID:
                    case Types.NCLOB:
                        throw new UnsupportedOperationException("Unable to export this data type");
                    case Types.NCHAR:
                    case Types.CHAR:
                    case Types.VARCHAR:
                    case Types.LONGVARCHAR:
                    default:
                        csvBuilder.append(value.toString());
                }
            }
        }
        csvBuilder.eol();
    }
}