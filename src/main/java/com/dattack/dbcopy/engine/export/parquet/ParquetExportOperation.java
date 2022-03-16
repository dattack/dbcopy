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
package com.dattack.dbcopy.engine.export.parquet;

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
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang.exception.NestableRuntimeException;
import org.apache.parquet.hadoop.ParquetWriter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.Reader;
import java.sql.SQLException;
import java.util.Objects;

/**
 * Data export operation in Parquet format.
 *
 * @author cvarela
 * @since 0.1
 */
class ParquetExportOperation implements ExportOperation {

    private static final Logger LOGGER = LoggerFactory.getLogger(ParquetExportOperation.class);
    private final transient ExportOperationBean bean;
    private final transient DataTransfer dataTransfer;
    private final transient Schema schema;
    private final transient DbCopyTaskResult taskResult;
    private final transient ThreadLocal<Visitor> visitorThreadLocal = new ThreadLocal<>();
    private final transient ParquetWriter<Object> writer;

    /* default */ ParquetExportOperation(final ExportOperationBean bean, final DataTransfer dataTransfer,
        final DbCopyTaskResult taskResult, final ParquetWriter<Object> writer, final Schema schema)
    {
        this.bean = bean;
        this.dataTransfer = dataTransfer;
        this.taskResult = taskResult;
        this.writer = writer;
        this.schema = schema;
    }

    @Override
    public Integer call() throws Exception {

        int totalExportedRows = 0;

        try {

            visitorThreadLocal.set(new Visitor());

            while (true) {
                final AbstractDataType<?>[] row = dataTransfer.transfer();
                if (Objects.isNull(row)) {
                    break;
                }
                write(row);
                taskResult.addProcessedRows(1);
                totalExportedRows++;
                if (totalExportedRows % bean.getBatchSize() == 0) {
                    LOGGER.debug("Exported rows: {}", totalExportedRows);
                }
            }

        } catch (Exception e) {
            LOGGER.error("Error {}: {}", writer, e.getMessage());
            throw e;
        } finally {
            // clean thread-local object
            visitorThreadLocal.remove();
        }

        return totalExportedRows;
    }

    /**
     * Default {@link DataTypeVisitor} implementation.
     */
    private static class Visitor implements DataTypeVisitor { //NOPMD

        private transient ColumnMetadata columnMetadata;
        private transient GenericRecord genericRecord;

        @Override
        public void visit(final BigDecimalType type) {
            if (type.isNotNull()) {
                final int scale = type.getValue().scale();
                if (scale == 0) {
                    put(type.getValue().longValue());
                } else {
                    put(type.getValue().doubleValue());
                }
            }
        }

        @Override
        public void visit(final BlobType type) throws SQLException {
            if (type.isNotNull()) {
                put(type.getValue().getBytes(1L, (int) type.getValue().length()));
            }
        }

        @Override
        public void visit(final BooleanType type) {
            genericRecord.put(getIndex(), type.getValue());
        }

        @Override
        public void visit(final ByteType type) {
            put(type.getValue());
        }

        @Override
        public void visit(final BytesType type) {
            put(type.getValue());
        }

        @Override
        public void visit(final ClobType type) throws SQLException {
            if (type.isNotNull()) {
                try (Reader reader = type.getValue().getCharacterStream()) {
                    put(IOUtils.toString(reader));
                } catch (IOException e) {
                    throw new NestableRuntimeException(e);
                }
            }
        }

        @Override
        public void visit(final DateType type) {
            // A date logical type annotates an Avro int, where the int stores the number of days from
            // the unix epoch, 1 January 1970 (ISO calendar).
            if (type.isNotNull()) {
                put(type.getValue().toLocalDate().toEpochDay());
            }
        }

        @Override
        public void visit(final DoubleType type) {
            put(type.getValue());
        }

        @Override
        public void visit(final FloatType type) {
            put(type.getValue());
        }

        @Override
        public void visit(final IntegerType type) {
            put(type.getValue());
        }

        @Override
        public void visit(final LongType type) {
            put(type.getValue());
        }

        @Override
        public void visit(final NClobType type) throws SQLException {
            if (type.isNotNull()) {
                put(type.getValue().getSubString(1L, (int) type.getValue().length()));
            }
        }

        @Override
        public void visit(final NStringType type) {
            put(type.getValue());
        }

        @Override
        public void visit(final NullType type) {
            putNull();
        }

        @Override
        public void visit(final ShortType type) {
            put(type.getValue());
        }

        @Override
        public void visit(final StringType type) {
            put(type.getValue());
        }

        @Override
        public void visit(final TimeType type) {
            // A time-millis logical type annotates an Avro int, where the int stores the number of
            // milliseconds after midnight, 00:00:00.000.
            if (type.isNotNull()) {
                put(type.getValue().getTime());
            }
        }

        @Override
        public void visit(final TimestampType type) {
            // A timestamp-millis logical type annotates an Avro long, where the long stores the number
            // of milliseconds from the unix epoch, 1 January 1970 00:00:00.000 UTC.
            if (type.isNotNull()) {
                put(type.getValue().getTime());
            }
        }

        @Override
        public void visit(final XmlType type) throws SQLException {
            if (type.isNotNull()) {
                put(type.getValue().getString());
            }
        }

        /* default */ void setColumnMetadata(final ColumnMetadata columnMetadata) {
            this.columnMetadata = columnMetadata;
        }

        private GenericRecord getGenericRecord() {
            return genericRecord;
        }

        /* default */ void setGenericRecord(final GenericRecord genericRecord) {
            this.genericRecord = genericRecord;
        }

        private int getIndex() {
            return columnMetadata.getIndex() - 1;
        }

        private void put(final Number value) {
            getGenericRecord().put(getIndex(), value);
        }

        private void put(final byte[] value) {
            getGenericRecord().put(getIndex(), value);
        }

        private void put(final String value) {
            getGenericRecord().put(getIndex(), value);
        }

        private void putNull() {
            getGenericRecord().put(getIndex(), null);
        }
    }

    private void write(final AbstractDataType<?>[] dataList) throws Exception {

        visitorThreadLocal.get().setGenericRecord(new GenericData.Record(schema));

        for (final ColumnMetadata columnMetadata : dataTransfer.getRowMetadata().getColumnsMetadata()) {

            visitorThreadLocal.get().setColumnMetadata(columnMetadata);

            final AbstractDataType<?> value = dataList[columnMetadata.getIndex() - 1];
            if (Objects.isNull(value)) {
                visitorThreadLocal.get().getGenericRecord().put(visitorThreadLocal.get().getIndex(), null);
            } else {
                value.accept(visitorThreadLocal.get());
            }
        }

        synchronized (writer) {
            writer.write(visitorThreadLocal.get().getGenericRecord());
        }
    }
}
