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
import com.dattack.dbcopy.engine.functions.BigDecimalFunction;
import com.dattack.dbcopy.engine.functions.BlobFunction;
import com.dattack.dbcopy.engine.functions.BooleanFunction;
import com.dattack.dbcopy.engine.functions.ByteFunction;
import com.dattack.dbcopy.engine.functions.BytesFunction;
import com.dattack.dbcopy.engine.functions.ClobFunction;
import com.dattack.dbcopy.engine.functions.DateFunction;
import com.dattack.dbcopy.engine.functions.DoubleFunction;
import com.dattack.dbcopy.engine.functions.FloatFunction;
import com.dattack.dbcopy.engine.functions.FunctionVisitor;
import com.dattack.dbcopy.engine.functions.IntegerFunction;
import com.dattack.dbcopy.engine.functions.LongFunction;
import com.dattack.dbcopy.engine.functions.NClobFunction;
import com.dattack.dbcopy.engine.functions.NStringFunction;
import com.dattack.dbcopy.engine.functions.NullFunction;
import com.dattack.dbcopy.engine.functions.ShortFunction;
import com.dattack.dbcopy.engine.functions.StringFunction;
import com.dattack.dbcopy.engine.functions.TimeFunction;
import com.dattack.dbcopy.engine.functions.TimestampFunction;
import com.dattack.dbcopy.engine.functions.XmlFunction;
import com.dattack.jtoolbox.commons.configuration.ConfigurationUtil;
import com.dattack.jtoolbox.io.IOUtils;
import org.apache.avro.LogicalTypes;
import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.commons.configuration.AbstractConfiguration;
import org.apache.commons.lang.exception.NestableRuntimeException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.avro.AvroParquetWriter;
import org.apache.parquet.hadoop.ParquetFileWriter;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.apache.parquet.hadoop.util.HadoopOutputFile;
import org.apache.parquet.io.OutputFile;
import java.io.IOException;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

/**
 * factory responsible for instantiating {@link ParquetExportOperation} objects.
 *
 * @author cvarela
 * @since 0.3
 */
public class ParquetExportOperationFactory implements ExportOperationFactory {

    private final transient ExportOperationBean bean;
    private final transient AbstractConfiguration configuration;
    private transient Schema schema;
    private transient ParquetWriter<Object> writer;

    public ParquetExportOperationFactory(final ExportOperationBean bean, final AbstractConfiguration configuration) {
        this.bean = bean;
        this.configuration = configuration;
    }

    @Override
    public ExportOperation createTask(final DataTransfer dataTransfer, final DbCopyTaskResult taskResult) {

        try {
            final ParquetWriter<Object> outputWriter = getWriter(); //NOPMD: resource can't be closed here
            final ParquetWriter<Object> writer = getWriter();

            taskResult.addOnEndCommand(() -> {
                        IOUtils.closeQuietly(outputWriter);
                        return null;
                    }
            );

            return new ParquetExportOperation(bean, dataTransfer, taskResult, outputWriter,
                    getSchema(dataTransfer.getRowMetadata()));

        } catch (Exception e) {
            throw new NestableRuntimeException(e);
        }
    }

    private synchronized Schema getSchema(RowMetadata rowMetadata) throws Exception {

        if (schema == null) {
            final List<Schema.Field> fieldList = new ArrayList<>();

            final Visitor visitor = new Visitor();
            for (final ColumnMetadata columnMetadata : rowMetadata.getColumnsMetadata()) {
                columnMetadata.getFunction().accept(visitor);
                final Schema columnSchema = visitor.getSchema();
                fieldList.add(new Schema.Field(columnMetadata.getName(), columnSchema, null, null));
            }

            schema = Schema.createRecord("row", "row doc", "com.dattack.ns", false);
            schema.setFields(fieldList);

            System.out.println("Schema: " + schema);
        }
        return schema;
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

    private synchronized ParquetWriter<Object> getWriter() throws IOException {

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
        return writer;
    }

    private static class Visitor implements FunctionVisitor {

        private transient Schema schema;

        public Schema getSchema() {
            return schema;
        }

        @Override
        public void visit(final BigDecimalFunction function) throws SQLException {
            if (function.getColumnMetadata().getScale() == 0) {
                schema = SchemaBuilder.nullable().longType();
            } else {
                schema = SchemaBuilder.nullable().doubleType();
            }
        }

        @Override
        public void visit(final BlobFunction function) throws SQLException {
            schema = SchemaBuilder.nullable().bytesType();
        }

        @Override
        public void visit(final BooleanFunction function) throws SQLException {
            schema = SchemaBuilder.nullable().booleanType();
        }

        @Override
        public void visit(final ByteFunction function) throws SQLException {
            schema = SchemaBuilder.nullable().intType();
        }

        @Override
        public void visit(final BytesFunction function) throws SQLException {
            schema = SchemaBuilder.nullable().bytesType();
        }

        @Override
        public void visit(final ClobFunction function) throws SQLException {
            schema = SchemaBuilder.nullable().stringType();
        }

        @Override
        public void visit(final DateFunction function) throws SQLException {
            schema = SchemaBuilder.nullable().type(LogicalTypes.date() //
                    .addToSchema(Schema.create(Schema.Type.INT)));
        }

        @Override
        public void visit(final DoubleFunction function) throws SQLException {
            schema = SchemaBuilder.nullable().doubleType();
        }

        @Override
        public void visit(final FloatFunction function) throws SQLException {
            schema = SchemaBuilder.nullable().floatType();
        }

        @Override
        public void visit(final IntegerFunction function) throws SQLException {
            schema = SchemaBuilder.nullable().intType();
        }

        @Override
        public void visit(final LongFunction function) throws SQLException {
            schema = SchemaBuilder.nullable().longType();
        }

        @Override
        public void visit(final NClobFunction function) throws SQLException {
            schema = SchemaBuilder.nullable().stringType();
        }

        @Override
        public void visit(final NStringFunction function) throws SQLException {
            schema = SchemaBuilder.nullable().stringType();
        }

        @Override
        public void visit(final NullFunction function) throws SQLException {
            schema = SchemaBuilder.nullable().stringType();
        }

        @Override
        public void visit(final ShortFunction function) throws SQLException {
            schema = SchemaBuilder.nullable().intType();
        }

        @Override
        public void visit(final StringFunction function) throws SQLException {
            schema = SchemaBuilder.nullable().stringType();
        }

        @Override
        public void visit(final TimeFunction function) throws SQLException {
            schema = SchemaBuilder.nullable().type(LogicalTypes.timeMillis() //
                    .addToSchema(Schema.create(Schema.Type.LONG)));
        }

        @Override
        public void visit(final TimestampFunction function) throws SQLException {
            schema = SchemaBuilder.nullable().type(LogicalTypes.timestampMillis() //
                    .addToSchema(Schema.create(Schema.Type.LONG)));
        }

        @Override
        public void visit(final XmlFunction function) throws SQLException {
            schema = SchemaBuilder.nullable().stringType();
        }
    }
}
