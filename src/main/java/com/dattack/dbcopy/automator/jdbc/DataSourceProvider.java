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
package com.dattack.dbcopy.automator.jdbc;

import com.dattack.dbcopy.automator.InsertStrategy;
import com.dattack.dbcopy.automator.ObjectName;
import com.dattack.dbcopy.automator.TableMetadata;
import com.dattack.formats.xml.FluentXmlWriter;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;

/**
 * Abstract provider used to access database metadata and execute functionality specific to a database type.
 *
 * @author cvarela
 * @since 0.3
 */
public abstract class DataSourceProvider {

    protected static final String PARTITION_VAR = "partition";

    protected static String getPartitionVar(TableMetadata tableMetadata) {
        return tableMetadata.getPartitionList().isEmpty() ? null : PARTITION_VAR;
    }

    private static void addColumns(Connection connection, ObjectName tableName,
        TableMetadata.TableMetadataBuilder builder) throws SQLException
    {
        try (ResultSet resultSet = connection.getMetaData().getColumns(tableName.getCatalogName(), //
                                                                       tableName.getSchemaName(), //
                                                                       tableName.getTableName(),  //
                                                                       null))
        {
            while (resultSet.next()) {
                builder.withColumn(resultSet.getString("COLUMN_NAME"), //
                                   resultSet.getInt("ORDINAL_POSITION"));
            }
        }
    }

    private static void addPrimaryKeys(Connection connection, ObjectName tableName,
        TableMetadata.TableMetadataBuilder builder) throws SQLException
    {
        try (ResultSet resultSet = connection.getMetaData().getPrimaryKeys(tableName.getCatalogName(),
                                                                           tableName.getSchemaName(),
                                                                           tableName.getTableName()))
        {
            while (resultSet.next()) {
                builder.withPrimaryKey(resultSet.getString("COLUMN_NAME"), //
                                       resultSet.getShort("KEY_SEQ"));
            }
        }
    }

    /**
     * Adds the partitioning configuration of a table to a TableMetadataBuilder.
     *
     * @param connection a connection to database
     * @param objectName the name of the table
     * @param builder    the builder
     * @throws SQLException if a database error occurs
     */
    protected abstract void addPartitions(Connection connection, ObjectName objectName,
        TableMetadata.TableMetadataBuilder builder) throws SQLException;

    protected abstract void addTableStats(Connection connection, ObjectName objectName,
        TableMetadata.TableMetadataBuilder builder) throws SQLException;

    public abstract String generateSelectSql(TableMetadata tableMetadata);

    public abstract InsertStrategy getInsertStrategy(boolean initialLoad);

    public abstract void writeXmlPartitions(FluentXmlWriter xml, TableMetadata tableMetadata);

    public final TableMetadata getTableMetadata(Connection connection, ObjectName objectName) throws SQLException {

        TableMetadata.TableMetadataBuilder builder = new TableMetadata.TableMetadataBuilder(objectName);

        addPrimaryKeys(connection, objectName, builder);
        addColumns(connection, objectName, builder);
        addPartitions(connection, objectName, builder);
        addTableStats(connection, objectName, builder);
        return builder.build();
    }
}
