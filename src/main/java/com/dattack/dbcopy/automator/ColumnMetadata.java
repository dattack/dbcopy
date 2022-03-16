/*
 * Copyright (c) 2022, The Dattack team (http://www.dattack.com)
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
package com.dattack.dbcopy.automator;

import org.apache.commons.lang3.builder.ToStringBuilder;

/**
 * Represents the metadata of a column of a table.
 *
 * @author cvarela
 * @since 0.3
 */
public class ColumnMetadata {

    private final String name;
    private final int keySeq;
    private final int ordinalPosition;
    private final int partitionSeq;

    public ColumnMetadata(final String name) {
        this(name, -1, -1, -1);
    }

    public ColumnMetadata(final String name, final int ordinalPosition, final int keySeq, final int partitionSeq) {
        this.name = name;
        this.keySeq = keySeq;
        this.ordinalPosition = ordinalPosition;
        this.partitionSeq = partitionSeq;
    }

    public int getKeySeq() {
        return keySeq;
    }

    public String getName() {
        return name;
    }

    public int getOrdinalPosition() {
        return ordinalPosition;
    }

    public int getPartitionSeq() {
        return partitionSeq;
    }

    public boolean isPrimaryKey() {
        return getKeySeq() > 0;
    }

    @Override
    public String toString() {
        return new ToStringBuilder(this) //
            .append("name", name) //
            .append("keySeq", keySeq) //
            .append("ordinalPosition", ordinalPosition) //
            .append("partitionSeq", partitionSeq) //
            .toString();
    }
}
