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

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

/**
 * Factory used to instantiate table mapping.
 *
 * @author cvarela
 * @since 0.3
 */
public class TableMappingFactory {

    private static final String DEFAULT_MAPPING_FILE = "automator-mapping.properties";
    private static final Logger LOGGER = LoggerFactory.getLogger(TableMappingFactory.class);

    private static final Properties globalProperties = loadGlobalMapping();

    private static Properties loadGlobalMapping() {
        final Properties properties = new Properties();
        try (final InputStream stream = TableMappingFactory.class.getClassLoader().getResourceAsStream(
            DEFAULT_MAPPING_FILE))
        {
            if (stream != null) {
                properties.load(stream);
            } else {
                LOGGER.info("Configuration file {} not found: unable to use global mappings", DEFAULT_MAPPING_FILE);
            }
        } catch (IOException e) {
            LOGGER.warn("Unable to load from configuration file {}: {}", DEFAULT_MAPPING_FILE, e.getMessage());
        }
        LOGGER.info("Global mappings: {}", properties);
        return properties;
    }

    public TableMapping build(TableMetadata sourceTable, TableMetadata targetTable,
        ReplicationConfiguration replicationConfig)
    {
        List<ColumnMapping> list = new ArrayList<>();

        for (ColumnMetadata targetColumnMetadata : targetTable.getColumnMetadataList()) {
            ColumnMapping columnMapping = getColumnMapping(sourceTable, replicationConfig, targetColumnMetadata);
            list.add(columnMapping);
        }
        return new TableMapping(sourceTable, targetTable, list, globalProperties);
    }

    private ColumnMapping getColumnMapping(TableMetadata sourceTable, final ReplicationConfiguration replicationConfig,
        final ColumnMetadata targetColumnMetadata)
    {
        ColumnMetadata sourceColumnMetadata =
            getSourceColumnMetadata(sourceTable, replicationConfig, targetColumnMetadata);

        if (sourceColumnMetadata == null) {
            String mapping = globalProperties.getProperty(targetColumnMetadata.getName());
            if (mapping == null) {
                LOGGER.error("Column without mapping (target column: {})", targetColumnMetadata.getName());
            } else {
                sourceColumnMetadata = new ColumnMetadata(targetColumnMetadata.getName());
            }
        }

        return new ColumnMapping(sourceColumnMetadata, targetColumnMetadata);
    }

    private ColumnMetadata getSourceColumnMetadata(TableMetadata sourceTable,
        final ReplicationConfiguration replicationConfig, final ColumnMetadata targetColumnMetadata)
    {
        ColumnMetadata sourceColumnMetadata = sourceTable.getColumnMetadata(targetColumnMetadata.getName());

        if (sourceColumnMetadata == null && replicationConfig != null) {
            String sourceName = replicationConfig.getMapping(targetColumnMetadata.getName());
            if (StringUtils.startsWith(sourceName, "@")) {
                // variable
                sourceColumnMetadata = new ColumnMetadata(sourceName);
            } else if (StringUtils.isNotBlank(sourceName)) {
                sourceColumnMetadata = sourceTable.getColumnMetadata(sourceName);
            }
        }

        return sourceColumnMetadata;
    }
}
