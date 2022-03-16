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

import com.dattack.dbcopy.automator.jdbc.DataSourceProvider;
import com.dattack.dbcopy.automator.jdbc.SinkDataSourceProvider;
import com.dattack.dbcopy.automator.jdbc.db2.Db2DataSourceProvider;
import com.dattack.dbcopy.automator.jdbc.oracle.OracleDataSourceProvider;
import com.dattack.jtoolbox.jdbc.JNDIDataSource;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang3.builder.ToStringBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import javax.sql.DataSource;

/**
 * Configuration of a database resource: it can be a single table or a complete database schema.
 *
 * @author cvarela
 * @since 0.3
 */
public final class DatabaseResource {

    private static final String DB2_PRODUCT_NAME = "DB2";
    private static final Logger LOGGER = LoggerFactory.getLogger(DatabaseResource.class);
    private static final String ORACLE_PRODUCT_NAME = "Oracle";
    private final ObjectName objectRef;
    private final String jndiName;
    private final DataSourceProvider provider;

    public DatabaseResource(ObjectName objectRef, String jndiName) throws SQLException {
        this(objectRef, jndiName, createProvider(jndiName));
    }

    private DatabaseResource(ObjectName objectRef, String jndiName, DataSourceProvider provider) {
        this.objectRef = objectRef;
        this.jndiName = jndiName;
        this.provider = provider;
    }

    public DatabaseResource(ObjectName objectRef, DatabaseResource parent) {
        this.objectRef = objectRef;
        this.jndiName = parent.getJndiName();
        this.provider = parent.getProvider();
    }

    private static DataSourceProvider createProvider(String jndiName) throws SQLException {
        try (Connection connection = new JNDIDataSource(jndiName).getConnection()) {
            return createProvider(connection);
        }
    }

    private static DataSourceProvider createProvider(final Connection connection) throws SQLException {
        DataSourceProvider result;
        String databaseProductName = connection.getMetaData().getDatabaseProductName();
        if (StringUtils.containsIgnoreCase(databaseProductName, ORACLE_PRODUCT_NAME)) {
            result = new OracleDataSourceProvider();
        } else if (StringUtils.containsIgnoreCase(databaseProductName, DB2_PRODUCT_NAME)) {
            result = new Db2DataSourceProvider();
        } else {
            result = new SinkDataSourceProvider();
        }
        LOGGER.debug("Using provider '{}' with database '{}' (URL: {})", result.getClass(), databaseProductName,
                     connection.getMetaData().getURL());
        return result;
    }

    public InsertStrategy createStrategy(final boolean initialLoad) {
        return getProvider().getInsertStrategy(initialLoad);
    }

    public DataSource getDataSource() {
        return new JNDIDataSource(getJndiName());
    }

    public String getJndiName() {
        return jndiName;
    }

    public ObjectName getObjectRef() {
        return objectRef;
    }

    public TableMetadata getTableMetadata() throws SQLException {
        try (Connection connection = getDataSource().getConnection()) {
            return getProvider().getTableMetadata(connection, getObjectRef());
        }
    }

    @Override
    public String toString() {
        return new ToStringBuilder(this) //
            .append("objectRef", objectRef) //
            .append("jndiName", jndiName) //
            .append("provider", provider).toString();
    }

    /* default */ DataSourceProvider getProvider() {
        return provider;
    }

    /* default */ List<ObjectName> listTables() {

        LOGGER.debug("[{}] Listing tables for {}", getJndiName(), getObjectRef());

        List<ObjectName> list = new ArrayList<>();
        try (Connection connection = getDataSource().getConnection();
             ResultSet rs = connection.getMetaData().getTables(getObjectRef().getCatalogName(), //
                                                               getObjectRef().getSchemaName(), //
                                                               getObjectRef().getTableName(), //
                                                               new String[]{ "TABLE" }))
        {
            while (rs.next()) {
                String catalogName = rs.getString("TABLE_CAT");
                String schemaName = rs.getString("TABLE_SCHEM");
                String tableName = rs.getString("TABLE_NAME");
                list.add(new ObjectName(catalogName, schemaName, tableName));
            }
        } catch (SQLException e) {
            LOGGER.error(e.getMessage(), e);
        }

        LOGGER.debug("[{}] {} tables found in {}", getJndiName(), list.size(), getObjectRef());
        return list;
    }
}
