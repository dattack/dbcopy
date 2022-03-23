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

import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang3.builder.ToStringBuilder;

import java.sql.Timestamp;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Function;

/**
 * Represents the metadata of a table.
 *
 * @author cvarela
 * @since 0.3
 */
public class TableMetadata {

    private final ObjectName objectName;
    private final List<ColumnMetadata> columnMetadataList;
    private final List<RangePartition> rangePartitionList;
    private final int numRows;
    private final Instant lastAnalyzed;

    private TableMetadata(ObjectName objectName, List<ColumnMetadata> columnMetadataList, int numRows,
        Instant lastAnalyzed, List<RangePartition> rangePartitionList)
    {
        this.objectName = objectName;
        this.numRows = numRows;
        this.lastAnalyzed = lastAnalyzed;
        this.columnMetadataList = columnMetadataList;
        this.rangePartitionList = rangePartitionList;
    }

    public ColumnMetadata getColumnMetadata(String columnName) {
        ColumnMetadata result = null;
        for (ColumnMetadata metadata : columnMetadataList) {
            if (columnName.equalsIgnoreCase(metadata.getName())) {
                result = metadata;
                break;
            }
        }
        return result;
    }

    public List<ColumnMetadata> getColumnMetadataList() {
        return Collections.unmodifiableList(columnMetadataList);
    }

    public RangePartition.InclusiveMode getHighInclusiveMode() {
        return getInclusiveMode(RangePartition::getHighInclusiveMode);
    }

    public Instant getLastAnalyzed() {
        return lastAnalyzed;
    }

    public RangePartition.InclusiveMode getLowInclusiveMode() {
        return getInclusiveMode(RangePartition::getLowInclusiveMode);
    }

    public int getNumRows() {
        return numRows;
    }

    public List<ColumnMetadata> getPartitionKeys() {
        List<ColumnMetadata> partitionKeys = new ArrayList<>();
        getColumnMetadataList().stream().filter(x -> x.getPartitionSeq() > 0).forEach(partitionKeys::add);
        return partitionKeys;
    }

    public List<RangePartition> getPartitionList() {
        return Collections.unmodifiableList(rangePartitionList);
    }

    public ObjectName getTableRef() {
        return objectName;
    }

    public boolean isAllPartitionsHasSize1() {
        Optional<RangePartition> partition = rangePartitionList.stream().filter(p -> !p.isSize1()).findAny();
        return !partition.isPresent();
    }

    @Override
    public String toString() {
        return new ToStringBuilder(this) //
            .append("tableRef", objectName) //
            .append("columnMetadataList", columnMetadataList) //
            .append("partitionList", rangePartitionList) //
            .toString();
    }

    public static class TableMetadataBuilder {

        private final ObjectName tableName;
        private final List<RangePartition> rangePartitionList;
        private final Map<String, Integer> columnMap;
        private final Map<String, Integer> primaryKeyMap;
        private final Map<String, Integer> partitionColumnMap;
        private int numRows;
        private Instant lastAnalyzed;

        public TableMetadataBuilder(ObjectName tableName) {
            this.tableName = tableName;
            this.numRows = 0;
            this.lastAnalyzed = Instant.ofEpochMilli(0L);
            this.rangePartitionList = new ArrayList<>();
            this.columnMap = new HashMap<>();
            this.primaryKeyMap = new HashMap<>();
            this.partitionColumnMap = new HashMap<>();
        }

        private static int toInt(Integer value) {
            int result = -1;
            if (value != null) {
                result = value;
            }
            return result;
        }

        public TableMetadata build() {
            return new TableMetadata(tableName, getColumnMetadataList(), numRows, lastAnalyzed, rangePartitionList);
        }

        public TableMetadataBuilder withColumn(String columnName, int ordinalPosition) {
            this.columnMap.put(norm(columnName), ordinalPosition);
            return this;
        }

        public TableMetadataBuilder withLastAnalyzed(Timestamp lastAnalyzed) {
            if (lastAnalyzed != null) {
                withLastAnalyzed(lastAnalyzed.toInstant());
            }
            return this;
        }

        public TableMetadataBuilder withLastAnalyzed(Instant lastAnalyzed) {
            this.lastAnalyzed = lastAnalyzed;
            return this;
        }

        public TableMetadataBuilder withNumRows(int numRows) {
            if (numRows >= 0) {
                this.numRows = numRows;
            }
            return this;
        }

        public TableMetadataBuilder withPartition(RangePartition partition) {
            this.rangePartitionList.add(partition);
            return this;
        }

        public TableMetadataBuilder withPartitionColumn(String columnName, int partitionSeq) {
            this.partitionColumnMap.put(norm(columnName), partitionSeq);
            return this;
        }

        public TableMetadataBuilder withPrimaryKey(String columnName, int keySeq) {
            this.primaryKeyMap.put(norm(columnName), keySeq);
            return this;
        }

        private List<ColumnMetadata> getColumnMetadataList() {
            List<ColumnMetadata> columnMetadataList = new ArrayList<>();
            for (Map.Entry<String, Integer> entrySet : columnMap.entrySet()) {
                Integer keySeq = primaryKeyMap.get(entrySet.getKey());
                Integer partitionSeq = partitionColumnMap.get(entrySet.getKey());
                columnMetadataList.add(new ColumnMetadata(entrySet.getKey(), toInt(entrySet.getValue()), toInt(keySeq),
                                                          toInt(partitionSeq)));
            }
            return columnMetadataList;
        }

        private String norm(String columnName) {
            return StringUtils.upperCase(columnName);
        }
    }

    private RangePartition.InclusiveMode getInclusiveMode(
        Function<RangePartition, RangePartition.InclusiveMode> function)
    {
        RangePartition.InclusiveMode result = RangePartition.InclusiveMode.UNSET;
        for (RangePartition rangePartition : rangePartitionList) {
            if (result.equals(RangePartition.InclusiveMode.UNSET)) {
                result = function.apply(rangePartition);
            } else if (!result.equals(function.apply(rangePartition))) {
                result = RangePartition.InclusiveMode.MIXED;
                break;
            }
        }
        return result;
    }
}
