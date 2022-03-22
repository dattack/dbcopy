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
 * Mapping of a column of the source table to a column of the target table. This mapping allows the columns to have
 * different names in the source table and the target table.
 *
 * @author cvarela
 * @since 0.3
 */
public class ColumnMapping {

    private final ColumnMetadata sourceColumn;
    private final ColumnMetadata targetColumn;

    public ColumnMapping(ColumnMetadata sourceColumn, ColumnMetadata targetColumn) {
        this.sourceColumn = sourceColumn;
        this.targetColumn = targetColumn;
    }

    public ColumnMetadata getSourceColumn() {
        return sourceColumn;
    }

    public ColumnMetadata getTargetColumn() {
        return targetColumn;
    }

    @Override
    public String toString() {
        return new ToStringBuilder(this) //
            .append("sourceColumn", sourceColumn) //
            .append("targetColumn", targetColumn) //
            .toString();
    }
}
