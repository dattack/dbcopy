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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

/**
 * Set of metadata corresponding to a row returned by {@link DataTransfer#transfer()}.
 *
 * @author cvarela
 * @since 0.1
 */
public class RowMetadata {

    private List<ColumnMetadata> columnsMetadata;

    public RowMetadata(List<ColumnMetadata> columnMetadataList) {
        ColumnMetadata[] array = new ColumnMetadata[columnMetadataList.size()];
        for (ColumnMetadata columnMetadata : columnMetadataList) {
            int zeroIndex = columnMetadata.getIndex() - 1;
            ColumnMetadata previous = array[zeroIndex];
            if (previous != null) {
                throw new IllegalArgumentException(String.format("Found a duplicate column index on row metadata " +
                        "[previous: %s, current: %s]", previous, columnMetadata));
            }
            array[zeroIndex] = columnMetadata;
        }
        this.columnsMetadata = Collections.unmodifiableList(Arrays.asList(array));
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
}
