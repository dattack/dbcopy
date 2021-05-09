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
import com.dattack.dbcopy.engine.functions.*;
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
import java.sql.SQLException;
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

        } catch (Exception e) {
            throw new NestableRuntimeException(e);
        }
    }

    private synchronized void initSchema(RowMetadata rowMetadata) throws Exception {

        if (schema == null) {
            List<Schema.Field> fieldList = new ArrayList<>();

            Visitor visitor = new Visitor();
            for (ColumnMetadata columnMetadata : rowMetadata.getColumnsMetadata()) {
                columnMetadata.getFunction().accept(visitor);
                Schema columnSchema = visitor.getSchema();
                fieldList.add(new Schema.Field(columnMetadata.getName(), columnSchema, null, null));
            }

            schema = Schema.createRecord("row", "row doc", "com.dattack.ns", false);
            schema.setFields(fieldList);

            System.out.println("Schema: " + schema);
        }
    }

    private CompressionCodecName getCompression() {

        CompressionCodecName compression;
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

    private static class Visitor implements FunctionVisitor {

        private Schema schema;

        public Schema getSchema() {
            return schema;
        }

        @Override
        public void visit(BigDecimalFunction function) throws SQLException {
            if (function.getColumnMetadata().getScale() == 0) {
                schema = SchemaBuilder.nullable().longType();
            } else {
                schema = SchemaBuilder.nullable().doubleType();
            }
        }

        @Override
        public void visit(BlobFunction function) throws SQLException {
            schema = SchemaBuilder.nullable().bytesType();
        }

        @Override
        public void visit(BooleanFunction function) throws SQLException {
            schema = SchemaBuilder.nullable().booleanType();
        }

        @Override
        public void visit(ByteFunction function) throws SQLException {
            schema = SchemaBuilder.nullable().intType();
        }

        @Override
        public void visit(BytesFunction function) throws SQLException {
            schema = SchemaBuilder.nullable().bytesType();
        }

        @Override
        public void visit(ClobFunction function) throws SQLException {
            schema = SchemaBuilder.nullable().stringType();
        }

        @Override
        public void visit(DateFunction function) throws SQLException {
            schema = SchemaBuilder.nullable().type(LogicalTypes.date() //
                    .addToSchema(Schema.create(Schema.Type.INT)));
        }

        @Override
        public void visit(DoubleFunction function) throws SQLException {
            schema = SchemaBuilder.nullable().doubleType();
        }

        @Override
        public void visit(FloatFunction function) throws SQLException {
            schema = SchemaBuilder.nullable().floatType();
        }

        @Override
        public void visit(IntegerFunction function) throws SQLException {
            schema = SchemaBuilder.nullable().intType();
        }

        @Override
        public void visit(LongFunction function) throws SQLException {
            schema = SchemaBuilder.nullable().longType();
        }

        @Override
        public void visit(NClobFunction function) throws SQLException {
            schema = SchemaBuilder.nullable().stringType();
        }

        @Override
        public void visit(NStringFunction function) throws SQLException {
            schema = SchemaBuilder.nullable().stringType();
        }

        @Override
        public void visit(NullFunction function) throws SQLException {
            schema = SchemaBuilder.nullable().stringType();
        }

        @Override
        public void visit(ShortFunction function) throws SQLException {
            schema = SchemaBuilder.nullable().intType();
        }

        @Override
        public void visit(StringFunction function) throws SQLException {
            schema = SchemaBuilder.nullable().stringType();
        }

        @Override
        public void visit(TimeFunction function) throws SQLException {
            schema = SchemaBuilder.nullable().type(LogicalTypes.timeMillis() //
                    .addToSchema(Schema.create(Schema.Type.LONG)));
        }

        @Override
        public void visit(TimestampFunction function) throws SQLException {
            schema = SchemaBuilder.nullable().type(LogicalTypes.timestampMillis() //
                    .addToSchema(Schema.create(Schema.Type.LONG)));
        }

        @Override
        public void visit(XmlFunction function) throws SQLException {
            schema = SchemaBuilder.nullable().stringType();
        }
    }
}
