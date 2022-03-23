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

import com.dattack.dbcopy.automator.jdbc.oracle.GoldenGateReplicatParser;
import com.dattack.formats.xml.FluentXmlWriter;
import com.dattack.jtoolbox.io.IOUtils;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.sql.SQLException;
import java.util.List;

import static com.dattack.dbcopy.automator.CodeHelper.EOL;
import static com.dattack.dbcopy.automator.CodeHelper.NTAB1;
import static com.dattack.dbcopy.automator.CodeHelper.NTAB2;
import static com.dattack.dbcopy.automator.CodeHelper.NTAB3;
import static com.dattack.dbcopy.automator.CodeHelper.TAB3;
import static com.dattack.dbcopy.automator.CodeHelper.reformat;

public class JobBuilder {

    private static final int DEFAULT_BATCH_SIZE = 10_000;
    private static final int DEFAULT_FETCH_SIZE = 20_000;
    private static final int DEFAULT_PARALLEL = 2;
    private static final int DEFAULT_THREADS_PER_JOB = 1;

    private static final Logger LOGGER = LoggerFactory.getLogger(JobBuilder.class);

    /**
     * Generates the configuration file with all the jobs needed to copy the data from a source database to a target
     * database. Both source and target can be a single table or a complete database schema.
     *
     * @param source           configuration of the source to be copied
     * @param target           configuration of the destination to write the copied data to
     * @param mappingDirectory directory containing the source and target table mapping files
     * @param initialLoad      when true, it assumes that the target table is empty and generates an INSERT
     *                         statement; otherwise, it generates a MERGE statement to update any records that may
     *                         already exist in the target table.
     * @param outputPath       the output file to generate
     * @throws IOException  if an I/O error occurs when accessing the mapping directory
     * @throws SQLException if an database error occurs
     */
    public void writeConfiguration(DatabaseResource source, DatabaseResource target, Path mappingDirectory,
        boolean initialLoad, Path outputPath) throws IOException, SQLException
    {
        final FluentXmlWriter xml = new FluentXmlWriter();
        xml.writeStartDocument("UTF-8", "1.0").writeCharacters(EOL) //
            .writeStartElement("dbcopy").writeAttribute("parallel", 1) //
            .writeCharacters(EOL);

        List<ObjectName> sourceTableList = source.listTables();
        List<ObjectName> targetTableList = target.listTables();

        for (ObjectName sourceTable : sourceTableList) {
            LOGGER.info("Analyzing source table '{}'", sourceTable);
            ReplicationConfiguration replicationConfig = findReplicationConfig(mappingDirectory, sourceTable);
            ObjectName targetTable = findTargetTable(sourceTable, replicationConfig, targetTableList);
            if (targetTable != null) {
                LOGGER.debug("Using table mapping (source: {} --> target: {})", sourceTable, targetTable);
                writeJobConfiguration(xml, new DatabaseResource(sourceTable, source), //
                                      new DatabaseResource(targetTable, target), //
                                      initialLoad, replicationConfig);
            } else {
                LOGGER.warn("The source table '{}' not exists on target database ({})", sourceTable,
                            target.getJndiName());
                xml.writeComment(String.format("%n%n%n Warning: the source table '%s' not " //
                                                   + "exists on target database (%s)%n%n%n", sourceTable,
                                               target.getJndiName()));
            }
        }
        xml.writeCharacters(EOL).writeEndElement().writeCharacters(EOL).writeEndDocument();

        try (
            OutputStream output = Files.newOutputStream(outputPath, StandardOpenOption.CREATE, StandardOpenOption.WRITE,
                                                        StandardOpenOption.TRUNCATE_EXISTING))
        {
            LOGGER.debug("Writing XML configuration to file {}", outputPath);
            output.write(xml.flush().getXml().getBytes(StandardCharsets.UTF_8));
            output.flush();
        }
    }

    private void writeJobConfiguration(FluentXmlWriter xml, DatabaseResource source, DatabaseResource target,
        boolean initialLoad, ReplicationConfiguration replicationConfig) throws SQLException
    {
        LOGGER.debug("{} - Generating job configuration", target.getObjectRef());
        xml.writeCharacters(NTAB1).writeComment(String.format("Stats: %s, num-rows: %,d, last-analyzed: %s", //
                                                              source.getTableMetadata().getTableRef(), //
                                                              source.getTableMetadata().getNumRows(), //
                                                              source.getTableMetadata().getLastAnalyzed())) //
            .writeCharacters(NTAB1).writeStartElement("job") // <job id=... threads=...>
            .writeAttribute("id", target.getObjectRef().toString()) //
            .writeAttribute("threads", DEFAULT_THREADS_PER_JOB);

        TableMapping tableMapping = getTableMappingMetadata(source, target, replicationConfig);
        writeTablePartitions(xml, source, tableMapping);
        writeSelectElement(xml, source, tableMapping);
        writeInsertElement(xml, target, tableMapping, initialLoad);

        // close job element
        xml.writeCharacters(NTAB1).writeEndElement(); // </job>
    }

    private void writeTablePartitions(FluentXmlWriter xml, DatabaseResource source, TableMapping tableMapping) {
        if (!tableMapping.getSourceTable().getPartitionList().isEmpty()) {
            LOGGER.debug("{} - Adding partitions configuration", source.getObjectRef());
            xml.writeCharacters(NTAB1);
            source.getProvider().writeXmlPartitions(xml, tableMapping.getSourceTable());
        }
    }

    private void writeSelectElement(FluentXmlWriter xml, DatabaseResource source, TableMapping tableMapping) {
        LOGGER.debug("{} - Adding SELECT configuration", source.getObjectRef());
        xml.writeCharacters(NTAB1).writeStartElement("select") // <select datasource=... fetch-size=...>
            .writeAttribute("datasource", source.getJndiName()) //
            .writeAttribute("fetch-size", DEFAULT_FETCH_SIZE) //
            // <![CDATA[ SELECT ... ]]>
            .writeCharacters(NTAB2).writeCData(NTAB3 + reformat(getSelectSql(source, tableMapping), TAB3)) //
            .writeCharacters(NTAB1).writeEndElement(); // </select>
    }

    private void writeInsertElement(FluentXmlWriter xml, DatabaseResource target, TableMapping tableMapping,
        boolean initialLoad)
    {
        LOGGER.debug("{} - Adding INSERT configuration", target.getObjectRef());
        xml.writeCharacters(NTAB1).writeStartElement("insert") // <insert datasource=... batch-size=... parallel=...>
            .writeAttribute("datasource", target.getJndiName()) //
            .writeAttribute("batch-size", DEFAULT_BATCH_SIZE) //
            .writeAttribute("parallel", DEFAULT_PARALLEL) //
            .writeCharacters(NTAB2).writeCData(
                NTAB3 + reformat(getInsertSql(target, initialLoad, tableMapping), TAB3)) //
            .writeCharacters(NTAB1).writeEndElement(); // </insert>
    }

    private String getSelectSql(DatabaseResource source, TableMapping tableMapping) {
        return source.getProvider().generateSelectSql(tableMapping.getSourceTable());
    }

    private String getInsertSql(DatabaseResource target, boolean initialLoad, TableMapping tableMapping) {
        return target.createStrategy(initialLoad).execute(target.getObjectRef(), tableMapping);
    }

    private ObjectName findTargetTable(ObjectName sourceTable, ReplicationConfiguration replicationConfig,
        List<ObjectName> targetTableList)
    {
        ObjectName targetTable = null;
        if (replicationConfig != null && targetTableList.contains(replicationConfig.getTargetTable())) {
            targetTable = replicationConfig.getTargetTable();
        } else {
            for (ObjectName objectName : targetTableList) {
                if (StringUtils.equalsIgnoreCase(objectName.getTableName(), sourceTable.getTableName())) {
                    targetTable = objectName;
                    break;
                }
            }
        }
        return targetTable;
    }

    private ReplicationConfiguration findReplicationConfig(Path directory, ObjectName sourceTable) throws IOException {
        if (directory != null) {
            LOGGER.debug("Searching for replication mapping file in {} and table {}", directory, sourceTable);
            for (Path path : IOUtils.listFiles(directory, sourceTable.getTableName())) {
                LOGGER.debug("Analyzing replication mapping file {}", path);
                ReplicationConfiguration replicationConfig = loadReplicationConfig(path);
                if (replicationConfig != null && sourceTable.equals(replicationConfig.getSourceTable())) {
                    LOGGER.debug("Replication mapping configuration {}", replicationConfig);
                    return replicationConfig;
                }
            }
        }
        LOGGER.debug("Replication mapping file not found for table {}", sourceTable);
        return null;
    }

    private ReplicationConfiguration loadReplicationConfig(Path path) throws IOException {
        ReplicationConfiguration replicationConfig = null;
        if (path != null) {
            replicationConfig = new GoldenGateReplicatParser().parse(path);
        }
        return replicationConfig;
    }

    private TableMapping getTableMappingMetadata(DatabaseResource source, DatabaseResource target,
        ReplicationConfiguration replicationConfig) throws SQLException
    {
        return new TableMappingFactory().build(source.getTableMetadata(), target.getTableMetadata(), replicationConfig);
    }
}
