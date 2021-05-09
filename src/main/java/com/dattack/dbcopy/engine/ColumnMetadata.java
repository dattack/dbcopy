/*
 * Copyright (c) 2019, The Dattack team (http://www.dattack.com)
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

import com.dattack.dbcopy.engine.functions.*;
import com.dattack.jtoolbox.patterns.Builder;

import java.sql.ResultSetMetaData;
import java.sql.Types;

/**
 * Set of metadata corresponding to a column returned by {@link DataTransfer#transfer()}.
 *
 * @author cvarela
 * @since 0.1
 */
public class ColumnMetadata {

    private final String name;
    private final int index;
    private final int type;
    private final int precision;
    private final int scale;
    private final boolean nullable;
    private AbstractDataFunction<?> function;

    private ColumnMetadata(ColumnMetadataBuilder builder) {
        this.name = builder.getName();
        this.index = builder.getIndex();
        this.type = builder.getType();
        this.precision = builder.getPrecision();
        this.scale = builder.getScale();
        this.nullable = builder.isNullable();
        createFunction();
    }

    private void createFunction() {

        switch (type) {

            case Types.BIGINT:
                this.function = new LongFunction(this);
                break;

            case Types.BINARY:
            case Types.VARBINARY:
            case Types.LONGVARBINARY:
                // BINARY, VARBINARY, LONGVARBINARY --> byte[]
                this.function = new BytesFunction(this);
                break;
            // Boolean

            case Types.BIT:
            case Types.BOOLEAN:
                // BIT, BOOLEAN --> Boolean
                function = new BooleanFunction(this);
                break;

            case Types.BLOB:
                function = new BlobFunction(this);
                break;

            case Types.CLOB:
                function = new ClobFunction(this);
                break;

            case Types.DATE:
                function = new DateFunction(this);
                break;

            case Types.DECIMAL:
            case Types.NUMERIC:
                // DECIMAL, NUMERIC --> BigDecimal
                function = new BigDecimalFunction(this);
                break;

            case Types.DOUBLE:
            case Types.FLOAT:
                // DOUBLE, FLOAT --> Double
                function = new DoubleFunction(this);
                break;

            case Types.INTEGER:
                function = new IntegerFunction(this);
                break;

            case Types.NCLOB:
                function = new NClobFunction(this);
                break;

            case Types.REAL:
                // REAL --> Float
                function = new FloatFunction(this);
                break;

            case Types.SMALLINT:
                // SMALLINT --> Short
                function = new ShortFunction(this);
                break;

            case Types.SQLXML:
                function = new XmlFunction(this);
                break;

            case Types.TIME:
            case Types.TIME_WITH_TIMEZONE:
                function = new TimeFunction(this);
                break;

            case Types.TIMESTAMP:
            case Types.TIMESTAMP_WITH_TIMEZONE:
                function = new TimestampFunction(this);
                break;

            case Types.TINYINT:
                function = new ByteFunction(this);
                break;

            case Types.NCHAR:
            case Types.LONGNVARCHAR:
            case Types.NVARCHAR:
                // NCHAR, LONGNVARCHAR, NVARCHAR --> NString
                function = new NStringFunction(this);
                break;

            case Types.CHAR:
            case Types.LONGVARCHAR:
            case Types.VARCHAR:
                // CHAR, LONGVARCHAR, VARCHAR --> String
                function = new StringFunction(this);
                break;

            case Types.ARRAY:
            case Types.NULL:
            case Types.OTHER:
            case Types.DATALINK:
            case Types.DISTINCT:
            case Types.JAVA_OBJECT:
            case Types.STRUCT:
            case Types.REF_CURSOR:
            case Types.ROWID:
            default:
                // unsupported data type
                function = new NullFunction(this);
                break;
        }
    }

    public int getIndex() {
        return index;
    }

    public int getType() {
        return type;
    }

    public String getName() {
        return name;
    }

    public int getPrecision() {
        return precision;
    }

    public int getScale() {
        return scale;
    }

    public boolean isNullable() {
        return nullable;
    }

    public AbstractDataFunction<?> getFunction() {
        return function;
    }

    @Override
    public String toString() {
        return "ColumnMetadata{" + //
                "name='" + getName() + '\'' + //
                ", index=" + getIndex() + //
                ", type=" + getType() + //
                ", precision=" + getPrecision() + //
                ", scale=" + getScale() + //
                ", nullable=" + isNullable() + //
                ", function=" + function + //
                '}';
    }

    public static class ColumnMetadataBuilder implements Builder<ColumnMetadata> {

        private String name;
        private int index;
        private int type;
        private int precision;
        private int scale;
        private int nullable;

        private int getIndex() {
            return index;
        }

        private int getType() {
            return type;
        }

        private String getName() {
            return name;
        }

        private int getPrecision() {
            return precision;
        }

        private int getScale() {
            return scale;
        }

        private boolean isNullable() {
            return nullable != ResultSetMetaData.columnNoNulls;
        }

        public ColumnMetadataBuilder withName(String value) {
            this.name = value;
            return this;
        }

        public ColumnMetadataBuilder withIndex(int value) {
            this.index = value;
            return this;
        }

        public ColumnMetadataBuilder withType(int value) {
            this.type = value;
            return this;
        }

        public ColumnMetadataBuilder withPrecision(int value) {
            this.precision = value;
            return this;
        }

        public ColumnMetadataBuilder withScale(int value) {
            this.scale = value;
            return this;
        }

        public ColumnMetadataBuilder withNullable(int value) {
            this.nullable = value;
            return this;
        }

        @Override
        public ColumnMetadata build() {
            return new ColumnMetadata(this);
        }
    }
}
