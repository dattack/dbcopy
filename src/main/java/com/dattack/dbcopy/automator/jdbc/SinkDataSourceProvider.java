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

/**
 * Default {@link DataSourceProvider} implementation for generic databases.
 *
 * @author cvarela
 * @since 0.3
 */
public class SinkDataSourceProvider extends DataSourceProvider {

    @Override
    public void addTableStats(final Connection connection, final ObjectName tableName,
        TableMetadata.TableMetadataBuilder builder)
    {
        // empty
    }

    @Override
    public String generateSelectSql(final TableMetadata tableMetadata) {
        return "SELECT * FROM " + tableMetadata.getTableRef();
    }

    @Override
    public InsertStrategy getInsertStrategy(final boolean initialLoad) {
        return new DefaultInsertStrategy();
    }

    @Override
    public void writeXmlPartitions(final FluentXmlWriter xml, final TableMetadata tableMetadata) {
        // empty
    }

    @Override
    protected void addPartitions(final Connection connection, final ObjectName tableName,
        TableMetadata.TableMetadataBuilder builder)
    {
        // empty
    }
}
