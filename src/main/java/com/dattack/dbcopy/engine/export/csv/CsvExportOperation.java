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
import com.dattack.dbcopy.engine.datatype.AbstractDataType;
import com.dattack.dbcopy.engine.datatype.BigDecimalType;
import com.dattack.dbcopy.engine.datatype.BlobType;
import com.dattack.dbcopy.engine.datatype.BooleanType;
import com.dattack.dbcopy.engine.datatype.ByteType;
import com.dattack.dbcopy.engine.datatype.BytesType;
import com.dattack.dbcopy.engine.datatype.ClobType;
import com.dattack.dbcopy.engine.datatype.DataTypeVisitor;
import com.dattack.dbcopy.engine.datatype.DateType;
import com.dattack.dbcopy.engine.datatype.DoubleType;
import com.dattack.dbcopy.engine.datatype.FloatType;
import com.dattack.dbcopy.engine.datatype.IntegerType;
import com.dattack.dbcopy.engine.datatype.LongType;
import com.dattack.dbcopy.engine.datatype.NClobType;
import com.dattack.dbcopy.engine.datatype.NStringType;
import com.dattack.dbcopy.engine.datatype.NullType;
import com.dattack.dbcopy.engine.datatype.ShortType;
import com.dattack.dbcopy.engine.datatype.StringType;
import com.dattack.dbcopy.engine.datatype.TimeType;
import com.dattack.dbcopy.engine.datatype.TimestampType;
import com.dattack.dbcopy.engine.datatype.XmlType;
import com.dattack.dbcopy.engine.export.ExportOperation;
import com.dattack.formats.csv.CSVConfiguration;
import com.dattack.formats.csv.CSVStringBuilder;
import com.dattack.jtoolbox.exceptions.DattackNestableRuntimeException;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.io.FileInputStream;
import java.io.IOException;
import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.sql.Clob;
import java.sql.SQLException;
import java.sql.SQLXML;
import java.util.Properties;

/**
 * data export operation in CSV format.
 *
 * @author cvarela
 * @since 0.1
 */
public class CsvExportOperation implements ExportOperation {

    private static final Logger LOGGER = LoggerFactory.getLogger(CsvExportOperation.class);

    private final ExportOperationBean bean;
    private final DataTransfer dataTransfer;
    private final DbCopyTaskResult taskResult;
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

        for (ColumnMetadata columnMetadata : dataTransfer.getRowMetadata().getColumnsMetadata()) {
            builder.append(columnMetadata.getName());
        }
        builder.eol();
        return builder.toString();
    }

    @SuppressWarnings("checkstyle:AbbreviationAsWordInName")
    private CSVStringBuilder createCSVStringBuilder() throws IOException {

        Properties properties = new Properties();
        if (StringUtils.isNotBlank(bean.getFormatFile())) {
            try (FileInputStream fis = new FileInputStream(bean.getFormatFile())) {
                properties.load(fis);
            }
        }
        CSVConfiguration configuration = CSVConfiguration.custom(properties).build();
        return new CSVStringBuilder(configuration);
    }

    @Override
    public Integer call() throws Exception {

        int totalExportedRows = 0;

        try {
            CSVStringBuilder csvStringBuilder = createCSVStringBuilder();
            writer.setHeader(getHeader(csvStringBuilder));
            csvStringBuilder.clear();

            Visitor visitor = new Visitor(csvStringBuilder);
            while (true) {
                AbstractDataType<?>[] row = dataTransfer.transfer();
                if (row == null) {
                    break;
                }
                populate(visitor, csvStringBuilder, row);
                taskResult.addProcessedRows(1);
                totalExportedRows++;
                if (totalExportedRows % bean.getBatchSize() == 0) {
                    LOGGER.debug("Exported rows: {}", totalExportedRows);
                    writer.write(csvStringBuilder.toString());
                    csvStringBuilder.clear();
                }
            }
            String line = csvStringBuilder.toString();
            writer.write(line);
        } catch (Exception e) {
            LOGGER.error("I/O error {}: {}", writer, e.getMessage());
            throw e;
        }

        return totalExportedRows;
    }


    private void populate(Visitor visitor, CSVStringBuilder csvStringBuilder, AbstractDataType<?>[] dataList)
            throws Exception {

        for (ColumnMetadata columnMetadata : dataTransfer.getRowMetadata().getColumnsMetadata()) {
            final AbstractDataType<?> value = dataList[columnMetadata.getIndex() - 1];
            if (value == null || value.isNull()) {
                csvStringBuilder.append((String) null);
            } else {
                value.accept(visitor);
            }
        }
        csvStringBuilder.eol();
    }

    private static class Visitor implements DataTypeVisitor {

        private final CSVStringBuilder csvStringBuilder;

        public Visitor(CSVStringBuilder csvStringBuilder) {
            this.csvStringBuilder = csvStringBuilder;
        }

        @Override
        public void visit(BigDecimalType value) {
            BigDecimal bigDecimal = value.getValue();
            int scale = bigDecimal.scale();
            if (scale == 0) {
                csvStringBuilder.append(bigDecimal.longValue());
            } else {
                csvStringBuilder.append(bigDecimal.doubleValue());
            }
        }

        @Override
        public void visit(BlobType value) {
            try {
                appendEncodedBytes(value.getValue().getBytes(0, (int) value.getValue().length()));
            } catch (SQLException e) {
                throw new DattackNestableRuntimeException(e);
            }
        }

        @Override
        public void visit(BooleanType value) {
            csvStringBuilder.append(value.getValue());
        }

        @Override
        public void visit(ByteType value) {
            csvStringBuilder.append(value.getValue().intValue());
        }

        @Override
        public void visit(BytesType value) {
            appendEncodedBytes(value.getValue());
        }

        @Override
        public void visit(ClobType value) {
            try {
                Clob clob = value.getValue();
                csvStringBuilder.append(clob.getSubString(0L, (int) clob.length()));
            } catch (SQLException e) {
                throw new DattackNestableRuntimeException(e);
            }
        }

        @Override
        public void visit(DateType value) {
            csvStringBuilder.append(value.getValue());
        }

        @Override
        public void visit(DoubleType value) {
            csvStringBuilder.append(value.getValue());
        }

        @Override
        public void visit(FloatType value) {
            csvStringBuilder.append(value.getValue());
        }

        @Override
        public void visit(IntegerType value) {
            csvStringBuilder.append(value.getValue());
        }

        @Override
        public void visit(LongType value) {
            csvStringBuilder.append(value.getValue());
        }

        @Override
        public void visit(NClobType value) {
            try {
                csvStringBuilder.append(value.getValue().getSubString(0, (int) value.getValue().length()));
            } catch (SQLException e) {
                throw new DattackNestableRuntimeException(e);
            }
        }

        @Override
        public void visit(NStringType value) {
            csvStringBuilder.append(value.getValue());
        }

        @Override
        public void visit(NullType value) {
            csvStringBuilder.append((String) null);
        }

        @Override
        public void visit(ShortType value) {
            csvStringBuilder.append(value.getValue().intValue());
        }

        @Override
        public void visit(StringType value) {
            csvStringBuilder.append(value.getValue());
        }

        @Override
        public void visit(TimeType value) {
            csvStringBuilder.append(value.getValue());
        }

        @Override
        public void visit(TimestampType value) {
            csvStringBuilder.append(value.getValue());
        }

        @Override
        public void visit(XmlType value) {
            try {
                SQLXML xml = value.getValue();
                csvStringBuilder.append(xml.getString());
            } catch (SQLException e) {
                throw new DattackNestableRuntimeException(e);
            }
        }

        private void appendEncodedBytes(byte[] bytes) {
            csvStringBuilder.append(new String(bytes, StandardCharsets.UTF_8));
        }
    }
}
