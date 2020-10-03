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
import com.dattack.dbcopy.engine.export.ExportOperation;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.parquet.hadoop.ParquetWriter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.math.BigDecimal;
import java.sql.*;
import java.util.Iterator;
import java.util.List;

/**
 * Executes the EXPORT operations.
 *
 * @author cvarela
 * @since 0.1
 */
class ParquetExportOperation implements ExportOperation {

    private final static Logger LOGGER = LoggerFactory.getLogger(ParquetExportOperation.class);

    private final ExportOperationBean bean;
    private final DataTransfer dataTransfer;
    private DbCopyTaskResult taskResult;
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
    public Integer call() throws SQLException, IOException, InterruptedException {

        int totalExportedRows = 0;

        try {
            while (true) {
                List<Object> row = dataTransfer.transfer();
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

        } catch (IOException e) {
            LOGGER.error("I/O error {}: {}", writer, e.getMessage());
            throw e;
        }

        return totalExportedRows;
    }

    private void write(List<Object> dataList) throws SQLException, IOException {

        Iterator<Object> dataIterator = dataList.iterator();

        GenericRecord genericRecord = new GenericData.Record(schema);

        for (ColumnMetadata columnMetadata: dataTransfer.getRowMetadata().getColumnsMetadata()) {

            int index = columnMetadata.getIndex() - 1;
            final Object value = dataIterator.next();
            if (value == null) {
                genericRecord.put(index, null);
            } else {
                switch (columnMetadata.getType()) {
                    case Types.CLOB:
                        String str;
                        if (value instanceof String) {
                            str = value.toString();
                        } else {
                            Clob clob = (Clob) value;
                            str = clob.getSubString(0L, (int) clob.length());
                        }
                        genericRecord.put(index, str);
                        break;
                    case Types.SQLXML:
                        SQLXML xml = (SQLXML) value;
                        genericRecord.put(index, xml.getString());
                        break;
                    case Types.BIT:
                        Boolean isTrue = ((Number) value).intValue() == 1;
                        genericRecord.put(index, isTrue);
                        break;
                    case Types.BOOLEAN:
                        genericRecord.put(index, (Boolean) value);
                        break;
                    case Types.DATE:
                        // A date logical type annotates an Avro int, where the int stores the number of days from
                        // the unix epoch, 1 January 1970 (ISO calendar).
                        Date date = (Date) value;
                        genericRecord.put(index, date.toLocalDate().toEpochDay());
                        break;
                    case Types.TIME:
                    case Types.TIME_WITH_TIMEZONE:
                        // A time-millis logical type annotates an Avro int, where the int stores the number of
                        // milliseconds after midnight, 00:00:00.000.
                        Time time = (Time) value;
                        genericRecord.put(index, time.getTime());
                        break;
                    case Types.TIMESTAMP:
                    case Types.TIMESTAMP_WITH_TIMEZONE:
                        // A timestamp-millis logical type annotates an Avro long, where the long stores the number
                        // of milliseconds from the unix epoch, 1 January 1970 00:00:00.000 UTC.
                        Timestamp timestamp = (Timestamp) value;
                        genericRecord.put(index, timestamp.getTime());
                        break;
                    case Types.DECIMAL:
                    case Types.NUMERIC:
                        BigDecimal bigDecimal = (BigDecimal) value;
                        int scale = bigDecimal.scale();
                         if (scale == 0) {
                             genericRecord.put(index, bigDecimal.longValue());
                         } else {
                             genericRecord.put(index, bigDecimal.doubleValue());
                         }
                        break;
                    case Types.REAL:
                    case Types.FLOAT:
                    case Types.TINYINT:
                    case Types.SMALLINT:
                    case Types.INTEGER:
                    case Types.DOUBLE:
                        genericRecord.put(index, (Number) value);
                        break;
                    case Types.BIGINT:
                        Number bigInteger = (Number) value;
                        genericRecord.put(index, bigInteger.longValue());
                        break;
                    case Types.BINARY:
                    case Types.VARBINARY:
                    case Types.LONGVARBINARY:
                        byte[] bytes = (byte[]) value;
                        genericRecord.put(index, bytes);
                        break;
                    case Types.BLOB:
                        throw new UnsupportedOperationException("Unable to export Blob type");
                    case Types.CHAR:
                    case Types.VARCHAR:
                    case Types.LONGVARCHAR:
                    default:
                        genericRecord.put(index, value.toString());
                }
            }
        }

        synchronized (writer) {
            writer.write(genericRecord);
        }
    }
}
