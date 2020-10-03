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

import com.dattack.jtoolbox.patterns.Builder;

import java.sql.ResultSetMetaData;

/**
 * Set of metadata corresponding to a column returned by {@link DataTransfer#transfer()}.
 *
 * @author cvarela
 * @since 0.1
 */
public class ColumnMetadata {

    private String name;
    private int index;
    private int type;
    private int precision;
    private int scale;
    private boolean nullable;

    private ColumnMetadata(ColumnMetadataBuilder builder) {
        this.name = builder.getName();
        this.index = builder.getIndex();
        this.type = builder.getType();
        this.precision = builder.getPrecision();
        this.scale = builder.getScale();
        this.nullable = builder.isNullable();
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

    @Override
    public String toString() {
        return new StringBuilder("ColumnMetadata{") //
                .append("name='").append(getName()).append('\'') //
                .append(", index=").append(getIndex()) //
                .append(", type=").append(getType()) //
                .append(", precision=").append(getPrecision()) //
                .append(", scale=").append(getScale()) //
                .append(", nullable=").append(isNullable()) //
                .append('}')
                .toString();
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
