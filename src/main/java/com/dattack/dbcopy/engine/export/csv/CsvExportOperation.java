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
import com.dattack.jtoolbox.exceptions.DattackParserException;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang.exception.NestableRuntimeException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.io.IOException;
import java.io.InputStream;
import java.io.Reader;
import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.sql.SQLException;
import java.sql.SQLXML;
import java.util.Objects;
import java.util.Properties;

/**
 * data export operation in CSV format.
 *
 * @author cvarela
 * @since 0.1
 */
public class CsvExportOperation implements ExportOperation {

    private static final Logger LOGGER = LoggerFactory.getLogger(CsvExportOperation.class);

    private final transient ExportOperationBean bean;
    private final transient DataTransfer dataTransfer;
    private final transient DbCopyTaskResult taskResult;
    private final transient CsvExportWriteWrapper writer;

    /* default */ CsvExportOperation(final ExportOperationBean bean, final DataTransfer dataTransfer,
                                     final DbCopyTaskResult taskResult,
                                     final CsvExportWriteWrapper writer) {
        this.bean = bean;
        this.dataTransfer = dataTransfer;
        this.taskResult = taskResult;
        this.writer = writer;
    }

    @Override
    public Integer call() throws Exception {

        int totalExportedRows = 0;

        try {
            final CSVStringBuilder csvStringBuilder = createCSVStringBuilder();
            writer.setHeader(getHeader(csvStringBuilder));
            csvStringBuilder.clear();

            final Visitor visitor = new Visitor(csvStringBuilder);
            while (true) {
                final AbstractDataType<?>[] row = dataTransfer.transfer();
                if (Objects.isNull(row)) {
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
            final String line = csvStringBuilder.toString();
            writer.write(line);
        } catch (Exception e) {
            LOGGER.error("I/O error {}: {}", writer, e.getMessage());
            throw e;
        }

        return totalExportedRows;
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

    private String getHeader(final CSVStringBuilder builder) {

        for (final ColumnMetadata columnMetadata : dataTransfer.getRowMetadata().getColumnsMetadata()) {
            builder.append(columnMetadata.getName());
        }
        builder.eol();
        return builder.toString();
    }

    private void populate(final Visitor visitor, final CSVStringBuilder csvStringBuilder,
                          final AbstractDataType<?>[] dataList) throws Exception {

        for (final ColumnMetadata columnMetadata : dataTransfer.getRowMetadata().getColumnsMetadata()) {
            final AbstractDataType<?> value = dataList[columnMetadata.getIndex() - 1];
            if (Objects.isNull(value) || value.isNull()) {
                csvStringBuilder.append((String) null);
            } else {
                value.accept(visitor);
            }
        }
        csvStringBuilder.eol();
    }

    /**
     * Default {@link DataTypeVisitor} implementation.
     */
    private static class Visitor implements DataTypeVisitor { //NOPMD

        private final transient CSVStringBuilder csvStringBuilder;

        public Visitor(final CSVStringBuilder csvStringBuilder) {
            this.csvStringBuilder = csvStringBuilder;
        }

        @Override
        public void visit(final BigDecimalType value) {
            final BigDecimal bigDecimal = value.getValue();
            final int scale = bigDecimal.scale();
            if (scale == 0) {
                csvStringBuilder.append(bigDecimal.longValue());
            } else {
                csvStringBuilder.append(bigDecimal.doubleValue());
            }
        }

        @Override
        public void visit(final BlobType value) {
            try {
                appendEncodedBytes(value.getValue().getBytes(1L, (int) value.getValue().length()));
            } catch (SQLException e) {
                throw new DattackNestableRuntimeException(e);
            }
        }

        @Override
        public void visit(final BooleanType value) {
            csvStringBuilder.append(value.getValue());
        }

        @Override
        public void visit(final ByteType value) {
            csvStringBuilder.append(value.getValue().intValue());
        }

        @Override
        public void visit(final BytesType value) {
            appendEncodedBytes(value.getValue());
        }

        @Override
        public void visit(final ClobType value) {
            try (Reader reader = value.getValue().getCharacterStream()) {
                csvStringBuilder.append(IOUtils.toString(reader));
            } catch (SQLException | IOException e) {
                throw new NestableRuntimeException(e);
            }
        }

        @Override
        public void visit(final DateType value) {
            csvStringBuilder.append(value.getValue());
        }

        @Override
        public void visit(final DoubleType value) {
            csvStringBuilder.append(value.getValue());
        }

        @Override
        public void visit(final FloatType value) {
            csvStringBuilder.append(value.getValue());
        }

        @Override
        public void visit(final IntegerType value) {
            csvStringBuilder.append(value.getValue());
        }

        @Override
        public void visit(final LongType value) {
            csvStringBuilder.append(value.getValue());
        }

        @Override
        public void visit(final NClobType value) {
            try {
                csvStringBuilder.append(value.getValue().getSubString(1L, (int) value.getValue().length()));
            } catch (SQLException e) {
                throw new DattackNestableRuntimeException(e);
            }
        }

        @Override
        public void visit(final NStringType value) {
            csvStringBuilder.append(value.getValue());
        }

        @Override
        public void visit(final NullType value) {
            csvStringBuilder.append((String) null);
        }

        @Override
        public void visit(final ShortType value) {
            csvStringBuilder.append(value.getValue().intValue());
        }

        @Override
        public void visit(final StringType value) {
            csvStringBuilder.append(value.getValue());
        }

        @Override
        public void visit(final TimeType value) {
            csvStringBuilder.append(value.getValue());
        }

        @Override
        public void visit(final TimestampType value) {
            csvStringBuilder.append(value.getValue());
        }

        @Override
        public void visit(final XmlType value) {
            try {
                final SQLXML xml = value.getValue();
                csvStringBuilder.append(xml.getString());
            } catch (SQLException e) {
                throw new DattackNestableRuntimeException(e);
            }
        }

        private void appendEncodedBytes(final byte[] bytes) {
            csvStringBuilder.append(new String(bytes, StandardCharsets.UTF_8));
        }
    }
}
