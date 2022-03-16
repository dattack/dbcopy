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
package com.dattack.dbcopy.automator.jdbc.db2;

import com.dattack.dbcopy.automator.CodeHelper;
import com.dattack.dbcopy.automator.InsertStrategy;
import com.dattack.dbcopy.automator.ObjectName;
import com.dattack.dbcopy.automator.RangePartition;
import com.dattack.dbcopy.automator.TableMetadata;
import com.dattack.dbcopy.automator.jdbc.DataSourceProvider;
import com.dattack.dbcopy.automator.jdbc.DefaultInsertStrategy;
import com.dattack.formats.xml.FluentXmlWriter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

/**
 * {@link DataSourceProvider} implementation for DB2 databases.
 *
 * @author cvarela
 * @since 0.3
 */
public class Db2DataSourceProvider extends DataSourceProvider {

    private static final Logger LOGGER = LoggerFactory.getLogger(Db2DataSourceProvider.class);

    private static final String SELECT_PARTITIONS = "SELECT dataPartitionName, lowValue, lowInclusive, highValue, " //
        + "highInclusive, dataPartitionId, card" //
        + " FROM syscat.dataPartitions" //
        + " WHERE tabSchema = ? AND tabName = ? WITH UR";

    private static final String SELECT_PARTITION_COLUMNS = "SELECT dataPartitionKeySeq, dataPartitionExpression FROM "//
        + "syscat.dataPartitionExpression WHERE tabschema = ? AND tabname = ? WITH UR";

    private static final String SELECT_TABLE_STATS = "SELECT tabName, card, stats_time" //
        + " FROM syscat.tables" //
        + " WHERE tabSchema = ? AND tabName = ? WITH UR";

    @Override
    public void addPartitions(Connection connection, ObjectName objectName,
        TableMetadata.TableMetadataBuilder builder) throws SQLException
    {

        try (PreparedStatement ps = connection.prepareStatement(SELECT_PARTITIONS)) {
            ps.setString(1, objectName.getSchemaName());
            ps.setString(2, objectName.getTableName());
            try (ResultSet rs = ps.executeQuery()) {
                while (rs.next()) {
                    String name = rs.getString("dataPartitionName");
                    String lowValue = rs.getString("lowValue");
                    Boolean lowInclusive = "Y".equalsIgnoreCase(rs.getString("lowInclusive"));
                    String highValue = rs.getString("highValue");
                    Boolean highInclusive = "Y".equalsIgnoreCase(rs.getString("highInclusive"));
                    int position = rs.getInt("dataPartitionId");
                    int numRows = rs.getInt("card");
                    builder.withPartition(
                        new RangePartition(name, lowValue, lowInclusive, highValue, highInclusive, position, numRows));
                }
            }
        }

        addPartitionColumns(connection, objectName, builder);
    }

    @Override
    public void addTableStats(Connection connection, ObjectName objectName,
        TableMetadata.TableMetadataBuilder builder) throws SQLException
    {
        LOGGER.trace("Retrieving table statistics ({})", objectName);
        try (PreparedStatement ps = connection.prepareStatement(SELECT_TABLE_STATS)) {
            ps.setString(1, objectName.getSchemaName());
            ps.setString(2, objectName.getTableName());
            try (ResultSet rs = ps.executeQuery()) {
                if (rs.next()) {
                    builder.withNumRows(rs.getInt("card")) //
                        .withLastAnalyzed(rs.getTimestamp("stats_time").toInstant());
                }
            }
        }
    }

    @Override
    public String generateSelectSql(TableMetadata tableMetadata) {
        LOGGER.trace("Generating SQL-Select for table {}", tableMetadata.getTableRef());
        String sql = CodeHelper.generateSelectSqlUsingRangePartitions(tableMetadata, getPartitionVar(tableMetadata)) //
            + " WITH UR";
        LOGGER.trace("SQL-Select for table {}: {}", tableMetadata.getTableRef(), sql);
        return sql;
    }

    @Override
    public InsertStrategy getInsertStrategy(final boolean initialLoad) {
        return new DefaultInsertStrategy();
    }

    @Override
    public void writeXmlPartitions(FluentXmlWriter xml, TableMetadata tableMetadata) {
        CodeHelper.appendPartitionRangeXml(xml, tableMetadata, getPartitionVar(tableMetadata));
    }

    private void addPartitionColumns(Connection connection, ObjectName objectName,
        TableMetadata.TableMetadataBuilder builder) throws SQLException
    {
        try (PreparedStatement ps = connection.prepareStatement(SELECT_PARTITION_COLUMNS)) {
            ps.setString(1, objectName.getSchemaName());
            ps.setString(2, objectName.getTableName());
            try (ResultSet rs = ps.executeQuery()) {
                while (rs.next()) {
                    int dataPartitionKeySeq = rs.getInt("dataPartitionKeySeq");
                    String dataPartitionExpression = rs.getString("dataPartitionExpression");
                    builder.withPartitionColumn(dataPartitionExpression, dataPartitionKeySeq);
                }
            }
        }
    }
}
