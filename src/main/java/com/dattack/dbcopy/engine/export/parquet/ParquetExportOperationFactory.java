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
package com.dattack.dbcopy.engine.export.parquet;

import com.dattack.dbcopy.beans.ExportOperationBean;
import com.dattack.dbcopy.engine.ColumnMetadata;
import com.dattack.dbcopy.engine.DataTransfer;
import com.dattack.dbcopy.engine.DbCopyTaskResult;
import com.dattack.dbcopy.engine.RowMetadata;
import com.dattack.dbcopy.engine.export.ExportOperation;
import com.dattack.dbcopy.engine.export.ExportOperationFactory;
import com.dattack.jtoolbox.commons.configuration.ConfigurationUtil;
import com.dattack.jtoolbox.io.IOUtils;
import org.apache.avro.LogicalTypes;
import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.commons.configuration.AbstractConfiguration;
import org.apache.commons.lang.exception.NestableRuntimeException;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.avro.AvroParquetWriter;
import org.apache.parquet.hadoop.ParquetFileWriter;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;

import java.io.IOException;
import java.sql.Types;
import java.util.ArrayList;
import java.util.List;

/**
 * @author cvarela
 * @since 0.3
 */
public class ParquetExportOperationFactory implements ExportOperationFactory {

    private final ExportOperationBean bean;
    private final AbstractConfiguration configuration;
    private Schema schema;
    private ParquetWriter<Object> writer;

    public ParquetExportOperationFactory(final ExportOperationBean bean, final AbstractConfiguration configuration) {
        this.bean = bean;
        this.configuration = configuration;
    }

    @Override
    public ExportOperation createTask(DataTransfer dataTransfer, DbCopyTaskResult taskResult) {

        try {
            initSchema(dataTransfer.getRowMetadata());
            initWriter();

            taskResult.addOnEndCommand(() -> {
                        IOUtils.closeQuietly(writer);
                        return null;
                    }
            );

            return new ParquetExportOperation(bean, dataTransfer, taskResult, writer, schema);

        } catch (IOException e) {
            throw new NestableRuntimeException(e);
        }
    }

    private synchronized void initSchema(RowMetadata rowMetadata) {

        if (schema == null) {
            List<Schema.Field> fieldList = new ArrayList<>();

            for (ColumnMetadata columnMetadata : rowMetadata.getColumnsMetadata()) {
                fieldList.add(new Schema.Field(columnMetadata.getName(), getType(columnMetadata), null, null));
            }

            schema = Schema.createRecord("row", "row doc", "com.dattack.ns", false);
            schema.setFields(fieldList);
        }
    }

    private CompressionCodecName getCompression() {

        CompressionCodecName compression = null;
        switch (bean.getCompression()) {
            case LZO:
                compression = CompressionCodecName.LZO;
                break;
            case GZIP:
                compression = CompressionCodecName.GZIP;
                break;
            case SNAPPY:
                compression = CompressionCodecName.SNAPPY;
                break;
            case UNCOMPRESSED:
            default:
                compression = CompressionCodecName.UNCOMPRESSED;
        }
        return compression;
    }

    private synchronized void initWriter() throws IOException {

        if (writer == null) {

            String filename = ConfigurationUtil.interpolate(bean.getPath(), configuration);
            Path hdfsPath = new Path(filename);

            writer = AvroParquetWriter.builder(hdfsPath)
                    .withSchema(schema) //
                    .withCompressionCodec(getCompression()) //
                    .withPageSize(bean.getPageSize()) //
                    .withWriteMode(ParquetFileWriter.Mode.OVERWRITE) //
                    .build();
        }
    }

    private static Schema getType(ColumnMetadata columnMetadata) {

        Schema type;
        switch (columnMetadata.getType()) {
            case Types.BIT:
            case Types.BOOLEAN:
                type = SchemaBuilder.nullable().booleanType();
                break;
            case Types.DATE:
                type = SchemaBuilder.nullable().type(LogicalTypes.date() //
                            .addToSchema(Schema.create(Schema.Type.INT)));
                break;
            case Types.TIME:
            case Types.TIME_WITH_TIMEZONE:
                type = SchemaBuilder.nullable().type(LogicalTypes.timeMillis() //
                            .addToSchema(Schema.create(Schema.Type.LONG)));
                break;
            case Types.TIMESTAMP:
            case Types.TIMESTAMP_WITH_TIMEZONE:
                type = SchemaBuilder.nullable().type(LogicalTypes.timestampMillis() //
                            .addToSchema(Schema.create(Schema.Type.LONG)));
                break;
            case Types.DECIMAL:
            case Types.NUMERIC:
                if (columnMetadata.getScale() == 0) {
                    type = SchemaBuilder.nullable().longType();
                } else {
                    type = SchemaBuilder.nullable().doubleType();
                }
                break;
            case Types.REAL:
            case Types.FLOAT:
                type = SchemaBuilder.nullable().floatType();
            case Types.TINYINT:
            case Types.SMALLINT:
            case Types.INTEGER:
                type = SchemaBuilder.nullable().intType();
                break;
            case Types.DOUBLE:
                type = SchemaBuilder.nullable().doubleType();
                break;
            case Types.BIGINT:
                type = SchemaBuilder.nullable().longType();
                break;
            case Types.BLOB:
                type = SchemaBuilder.nullable().bytesType();
                break;
            case Types.CHAR:
            case Types.VARCHAR:
            case Types.LONGVARCHAR:
            case Types.CLOB:
            case Types.SQLXML:
            default:
                type = SchemaBuilder.nullable().stringType();
        }
        return type;
    }
}
