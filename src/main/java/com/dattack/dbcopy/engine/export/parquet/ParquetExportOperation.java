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
import com.dattack.dbcopy.engine.datatype.*;
import com.dattack.dbcopy.engine.export.ExportOperation;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.parquet.hadoop.ParquetWriter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.SQLException;

/**
 * Executes the EXPORT operations.
 *
 * @author cvarela
 * @since 0.1
 */
class ParquetExportOperation implements ExportOperation {

    private final static Logger LOGGER = LoggerFactory.getLogger(ParquetExportOperation.class);

    private final ThreadLocal<Visitor> visitorThreadLocal = new ThreadLocal<>();

    private final ExportOperationBean bean;
    private final DataTransfer dataTransfer;
    private final DbCopyTaskResult taskResult;
    private final ParquetWriter<Object> writer;
    private final Schema schema;

    ParquetExportOperation(final ExportOperationBean bean, final DataTransfer dataTransfer,
                           final DbCopyTaskResult taskResult,
                           final ParquetWriter<Object> writer,
                           final Schema schema) {
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
                AbstractDataType<?>[] row = dataTransfer.transfer();
                if (row == null) {
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

    private void write(AbstractDataType<?>[] dataList) throws Exception {

        visitorThreadLocal.get().setGenericRecord(new GenericData.Record(schema));

        for (ColumnMetadata columnMetadata : dataTransfer.getRowMetadata().getColumnsMetadata()) {

            visitorThreadLocal.get().setColumnMetadata(columnMetadata);

            final AbstractDataType<?> value = dataList[columnMetadata.getIndex() - 1];
            if (value == null) {
                visitorThreadLocal.get().getGenericRecord().put(visitorThreadLocal.get().getIndex(), null);
            } else {
                value.accept(visitorThreadLocal.get());
            }
        }

        synchronized (writer) {
            writer.write(visitorThreadLocal.get().getGenericRecord());
        }
    }

    private static class Visitor implements DataTypeVisitor {

        private GenericRecord genericRecord;
        private ColumnMetadata columnMetadata;

        void setColumnMetadata(ColumnMetadata columnMetadata) {
            this.columnMetadata = columnMetadata;
        }

        void setGenericRecord(GenericRecord genericRecord) {
            this.genericRecord = genericRecord;
        }

        private int getIndex() {
            return columnMetadata.getIndex() - 1;
        }

        private GenericRecord getGenericRecord() {
            return genericRecord;
        }

        private void put(Number value) {
            getGenericRecord().put(getIndex(), value);
        }

        private void put(String value) {
            getGenericRecord().put(getIndex(), value);
        }

        private void put(byte[] value) {
            getGenericRecord().put(getIndex(), value);
        }

        private void putNull() {
            getGenericRecord().put(getIndex(), null);
        }

        @Override
        public void visit(BigDecimalType type) {
            if (type.isNotNull()) {
                int scale = type.getValue().scale();
                if (scale == 0) {
                    put(type.getValue().longValue());
                } else {
                    put(type.getValue().doubleValue());
                }
            }
        }

        @Override
        public void visit(BlobType type) throws SQLException {
            if (type.isNotNull()) {
                put(type.getValue().getBytes(0, (int) type.getValue().length()));
            }
        }

        @Override
        public void visit(BooleanType type) {
            genericRecord.put(getIndex(), type.getValue());
        }

        @Override
        public void visit(ByteType type) {
            put(type.getValue());
        }

        @Override
        public void visit(BytesType type) {
            put(type.getValue());
        }

        @Override
        public void visit(ClobType type) throws SQLException {
            if (type.isNotNull()) {
                put(type.getValue().getSubString(0L, (int) type.getValue().length()));
            }
        }

        @Override
        public void visit(DateType type) {
            // A date logical type annotates an Avro int, where the int stores the number of days from
            // the unix epoch, 1 January 1970 (ISO calendar).
            if (type.isNotNull()) {
                put(type.getValue().toLocalDate().toEpochDay());
            }
        }

        @Override
        public void visit(DoubleType type) {
            put(type.getValue());
        }

        @Override
        public void visit(FloatType type) {
            put(type.getValue());
        }

        @Override
        public void visit(IntegerType type) {
            put(type.getValue());
        }

        @Override
        public void visit(LongType type) {
            put(type.getValue());
        }

        @Override
        public void visit(NClobType type) throws SQLException {
            if (type.isNotNull()) {
                put(type.getValue().getSubString(0L, (int) type.getValue().length()));
            }
        }

        @Override
        public void visit(NStringType type) {
            put(type.getValue());
        }

        @Override
        public void visit(NullType type) {
            putNull();
        }

        @Override
        public void visit(ShortType type) {
            put(type.getValue());
        }

        @Override
        public void visit(StringType type) {
            put(type.getValue());
        }

        @Override
        public void visit(TimeType type) {
            // A time-millis logical type annotates an Avro int, where the int stores the number of
            // milliseconds after midnight, 00:00:00.000.
            if (type.isNotNull()) {
                put(type.getValue().getTime());
            }
        }

        @Override
        public void visit(TimestampType type) {
            // A timestamp-millis logical type annotates an Avro long, where the long stores the number
            // of milliseconds from the unix epoch, 1 January 1970 00:00:00.000 UTC.
            if (type.isNotNull()) {
                put(type.getValue().getTime());
            }
        }

        @Override
        public void visit(XmlType type) throws SQLException {
            if (type.isNotNull()) {
                put(type.getValue().getString());
            }
        }
    }
}
