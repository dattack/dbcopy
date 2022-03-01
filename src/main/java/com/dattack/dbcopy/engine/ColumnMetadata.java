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

import com.dattack.dbcopy.engine.functions.AbstractDataFunction;
import com.dattack.dbcopy.engine.functions.BigDecimalFunction;
import com.dattack.dbcopy.engine.functions.BlobFunction;
import com.dattack.dbcopy.engine.functions.BooleanFunction;
import com.dattack.dbcopy.engine.functions.ByteFunction;
import com.dattack.dbcopy.engine.functions.BytesFunction;
import com.dattack.dbcopy.engine.functions.ClobFunction;
import com.dattack.dbcopy.engine.functions.DateFunction;
import com.dattack.dbcopy.engine.functions.DoubleFunction;
import com.dattack.dbcopy.engine.functions.FloatFunction;
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

    private final AbstractDataFunction<?> function;
    private final int index;
    private final String name;
    private final boolean nullable;
    private final int precision;
    private final int scale;
    private final int type;

    private ColumnMetadata(final ColumnMetadataBuilder builder) {
        this.name = builder.getName();
        this.index = builder.getIndex();
        this.type = builder.getType();
        this.precision = builder.getPrecision();
        this.scale = builder.getScale();
        this.nullable = builder.isNullable();
        this.function = createFunction();
    }

    private AbstractDataFunction<?> createFunction() { //NOPMD

        AbstractDataFunction<?> result;
        switch (type) {

            case Types.BIGINT:
                result = new LongFunction(this);
                break;

            case Types.BINARY:
            case Types.VARBINARY:
            case Types.LONGVARBINARY:
                result = new BytesFunction(this);
                break;

            case Types.BIT:
            case Types.BOOLEAN:
                result = new BooleanFunction(this);
                break;

            case Types.BLOB:
                result = new BlobFunction(this);
                break;

            case Types.CLOB:
                result = new ClobFunction(this);
                break;

            case Types.DATE:
                result = new DateFunction(this);
                break;

            case Types.DECIMAL:
            case Types.NUMERIC:
                result = new BigDecimalFunction(this);
                break;

            case Types.DOUBLE:
            case Types.FLOAT:
                result = new DoubleFunction(this);
                break;

            case Types.INTEGER:
                result = new IntegerFunction(this);
                break;

            case Types.NCLOB:
                result = new NClobFunction(this);
                break;

            case Types.REAL:
                result = new FloatFunction(this);
                break;

            case Types.SMALLINT:
                result = new ShortFunction(this);
                break;

            case Types.SQLXML:
                result = new XmlFunction(this);
                break;

            case Types.TIME:
            case Types.TIME_WITH_TIMEZONE:
                result = new TimeFunction(this);
                break;

            case Types.TIMESTAMP:
            case Types.TIMESTAMP_WITH_TIMEZONE:
                result = new TimestampFunction(this);
                break;

            case Types.TINYINT:
                result = new ByteFunction(this);
                break;

            case Types.NCHAR:
            case Types.LONGNVARCHAR:
            case Types.NVARCHAR:
                result = new NStringFunction(this);
                break;

            case Types.CHAR:
            case Types.LONGVARCHAR:
            case Types.VARCHAR:
                result = new StringFunction(this);
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
                result = new NullFunction(this);
                break;
        }
        return result;
    }

    public static ColumnMetadataBuilder custom() {
        return new ColumnMetadataBuilder();
    }

    public AbstractDataFunction<?> getFunction() {
        return function;
    }

    @Override
    public String toString() {
        return "ColumnMetadata{" //
                + "name='" + getName() + '\'' //
                + ", index=" + getIndex() //
                + ", type=" + getType() //
                + ", precision=" + getPrecision() //
                + ", scale=" + getScale() //
                + ", nullable=" + isNullable() //
                + ", function=" + function //
                + '}';
    }

    public String getName() {
        return name;
    }

    public int getIndex() {
        return index;
    }

    public int getType() {
        return type;
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

    /**
     * Builder implementation for {@link ColumnMetadata}.
     */
    public static class ColumnMetadataBuilder implements Builder<ColumnMetadata> {

        private transient int index;
        private transient String name;
        private transient int nullable;
        private transient int precision;
        private transient int scale;
        private transient int type;

        private ColumnMetadataBuilder() {
            // private
        }

        @Override
        public ColumnMetadata build() {
            return new ColumnMetadata(this);
        }

        public ColumnMetadataBuilder withIndex(final int value) {
            this.index = value;
            return this;
        }

        public ColumnMetadataBuilder withName(final String value) {
            this.name = value;
            return this;
        }

        public ColumnMetadataBuilder withNullable(final int value) {
            this.nullable = value;
            return this;
        }

        public ColumnMetadataBuilder withPrecision(final int value) {
            this.precision = value;
            return this;
        }

        public ColumnMetadataBuilder withScale(final int value) {
            this.scale = value;
            return this;
        }

        public ColumnMetadataBuilder withType(final int value) {
            this.type = value;
            return this;
        }

        private int getIndex() {
            return index;
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

        private int getType() {
            return type;
        }

        private boolean isNullable() {
            return nullable != ResultSetMetaData.columnNoNulls;
        }
    }
}
