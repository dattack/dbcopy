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
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * Set of metadata corresponding to a row returned by {@link DataTransfer#transfer()}.
 *
 * @author cvarela
 * @since 0.1
 */
public class RowMetadata {

    private final transient List<ColumnMetadata> columnsMetadata;

    private RowMetadata(final List<ColumnMetadata> columnsMetadata) {
        this.columnsMetadata = columnsMetadata;
    }

    public static RowMetadataBuilder custom() {
        return new RowMetadataBuilder();
    }

    public int getColumnCount() {
        return columnsMetadata.size();
    }

    public List<ColumnMetadata> getColumnsMetadata() {
        return columnsMetadata;
    }

    @Override
    public String toString() {
        return "RowMetadata{columnsMetadata=" + columnsMetadata + '}';
    }

    /**
     * Builder implementation for {@link RowMetadata}.
     */
    public static class RowMetadataBuilder implements Builder<RowMetadata> {

        private final transient List<ColumnMetadata> list;

        private RowMetadataBuilder() {
            this.list = new ArrayList<>();
        }

        public RowMetadataBuilder add(final ColumnMetadata item) {
            list.add(item);
            return this;
        }

        @Override
        public RowMetadata build() {
            checkIndexes();
            return new RowMetadata(Collections.unmodifiableList(list));
        }

        private void checkIndexes() {
            ColumnMetadata[] array = new ColumnMetadata[list.size()];
            for (final ColumnMetadata columnMetadata : list) {
                final int zeroIndex = columnMetadata.getIndex() - 1;
                final ColumnMetadata previous = array[zeroIndex];
                if (previous != null) {
                    throw new IllegalArgumentException(String.format("Found a duplicate column index on row metadata "
                            + "[previous: %s, current: %s]", previous, columnMetadata));
                }
                array[zeroIndex] = columnMetadata;
            }
        }
    }
}
