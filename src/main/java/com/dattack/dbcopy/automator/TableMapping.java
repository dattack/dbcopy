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

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Properties;

/**
 * Mapping of a source table to a target table. The two tables may have different names or be created in different
 * schemas.
 *
 * @author cvarela
 * @since 0.3
 */
public class TableMapping {

    private final TableMetadata sourceTable;
    private final TableMetadata targetTable;
    private final List<ColumnMapping> columnMappingList;
    private final Properties globalMapping;

    public TableMapping(TableMetadata sourceTable, TableMetadata targetTable, List<ColumnMapping> columnMappingList,
        Properties globalMapping)
    {
        this.sourceTable = sourceTable;
        this.targetTable = targetTable;
        this.columnMappingList = new ArrayList<>(columnMappingList);
        this.globalMapping = new Properties(globalMapping);
    }

    public List<ColumnMapping> getColumnMappingList() {
        return Collections.unmodifiableList(columnMappingList);
    }

    public Map<?, ?> getGlobalMapping() {
        return Collections.unmodifiableMap(globalMapping);
    }

    public TableMetadata getSourceTable() {
        return sourceTable;
    }

    public TableMetadata getTargetTable() {
        return targetTable;
    }

    @Override
    public String toString() {
        return new ToStringBuilder(this) //
            .append("sourceTable", sourceTable) //
            .append("targetTable", targetTable) //
            .append("columnMappingList", columnMappingList) //
            .append("globalMapping", globalMapping) //
            .toString();
    }
}
