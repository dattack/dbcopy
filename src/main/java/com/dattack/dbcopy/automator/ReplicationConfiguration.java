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

import java.util.HashMap;
import java.util.Map;

/**
 * Replication configuration between a source table and a target database table.
 *
 * @author cvarela
 * @since 0.3
 */
public class ReplicationConfiguration {

    private final ObjectName sourceTable;
    private final ObjectName targetTable;
    private final Map<String, String> targetSourceMapping;

    public ReplicationConfiguration(String sourceTable, String targetTable) {
        this.sourceTable = ObjectName.parse(sourceTable);
        this.targetTable = ObjectName.parse(targetTable);
        this.targetSourceMapping = new HashMap<>();
    }

    public void addMapping(String targetColumn, String sourceColumn) {
        targetSourceMapping.put(targetColumn, sourceColumn);
    }

    public String getMapping(String targetName) {
        return targetSourceMapping.get(targetName);
    }

    public ObjectName getSourceTable() {
        return sourceTable;
    }

    public ObjectName getTargetTable() {
        return targetTable;
    }

    @Override
    public String toString() {
        return "MappingConfiguration{" //
            + "sourceTable='" + getSourceTable() + '\'' //
            + ", targetTable='" + getTargetTable() + '\'' //
            + ", mapping=" + targetSourceMapping + '}';
    }
}
