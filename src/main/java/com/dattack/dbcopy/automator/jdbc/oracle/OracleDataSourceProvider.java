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
package com.dattack.dbcopy.automator.jdbc.oracle;

import com.dattack.dbcopy.automator.CodeHelper;
import com.dattack.dbcopy.automator.InsertStrategy;
import com.dattack.dbcopy.automator.ObjectName;
import com.dattack.dbcopy.automator.RangePartition;
import com.dattack.dbcopy.automator.TableMetadata;
import com.dattack.dbcopy.automator.jdbc.DataSourceProvider;
import com.dattack.dbcopy.automator.jdbc.DefaultInsertStrategy;
import com.dattack.formats.xml.FluentXmlWriter;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

/**
 * {@link DataSourceProvider} implementation for Oracle databases.
 *
 * @author cvarela
 * @since 0.3
 */
public class OracleDataSourceProvider extends DataSourceProvider {

    private static final Logger LOGGER = LoggerFactory.getLogger(OracleDataSourceProvider.class);

    private static final String SELECT_PARTITIONS = "SELECT partition_name, high_value, partition_position, num_rows" //
        + " FROM all_tab_partitions" //
        + " WHERE table_owner = ? AND table_name = ?";

    private static final String SELECT_TABLE_STATS = "SELECT table_name, num_rows, last_analyzed" //
        + " FROM all_tables" //
        + " WHERE owner = ? AND table_name = ?";

    @Override
    public void addPartitions(Connection connection, ObjectName objectName,
        TableMetadata.TableMetadataBuilder builder) throws SQLException
    {
        try (PreparedStatement ps = connection.prepareStatement(SELECT_PARTITIONS)) {
            ps.setString(1, objectName.getSchemaName());
            ps.setString(2, objectName.getTableName());
            try (ResultSet rs = ps.executeQuery()) {
                while (rs.next()) {
                    String name = rs.getString("PARTITION_NAME");
                    String highValue = rs.getString("HIGH_VALUE");
                    int position = rs.getInt("PARTITION_POSITION");
                    int numRows = rs.getInt("NUM_ROWS");
                    builder.withPartition(new RangePartition(name, highValue, position, numRows));
                }
            }
        }
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
                    builder.withNumRows(rs.getInt("NUM_ROWS")) //
                        .withLastAnalyzed(rs.getTimestamp("LAST_ANALYZED").toInstant());
                }
            }
        }
    }

    @Override
    public String generateSelectSql(TableMetadata tableMetadata) {

        String sql = "SELECT * FROM " + tableMetadata.getTableRef();
        if (!tableMetadata.getPartitionList().isEmpty()) {
            sql += " PARTITION(${" + StringUtils.trim(getPartitionVar(tableMetadata)) + "})";
        }
        return sql;
    }

    @Override
    public InsertStrategy getInsertStrategy(final boolean initialLoad) {
        return initialLoad ? new DefaultInsertStrategy() : new OracleMergeStrategy();
    }

    @Override
    public void writeXmlPartitions(FluentXmlWriter xml, TableMetadata tableMetadata) {
        CodeHelper.appendPartitionListXml(xml, tableMetadata, getPartitionVar(tableMetadata));
    }
}
