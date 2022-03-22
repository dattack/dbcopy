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

import com.dattack.jtoolbox.util.function.FunctionHelper;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;

import java.util.ArrayList;
import java.util.List;

/**
 * An object name representation that can be used to reference either a table or a database schema or catalog.
 *
 * @author cvarela
 * @since 0.3
 */
public class ObjectName {

    private final String catalogName;
    private final String schemaName;
    private final String tableName;

    public ObjectName(String catalogName, String schemaName, String tableName) {
        this.catalogName = catalogName;
        this.schemaName = schemaName;
        this.tableName = tableName;
    }

    public static ObjectName parse(String text) {
        if (text == null) {
            return null;
        }
        String[] tokens = text.split("\\.");
        int index = tokens.length;
        String tableName = tokens[--index];
        String schemaName = index > 0 ? tokens[--index] : null;
        String catalogName = index > 0 ? tokens[--index] : null;
        return new ObjectName(catalogName, schemaName, tableName);
    }

    @Override
    public boolean equals(final Object o) {
        if (this == o) {
            return true;
        }

        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        final ObjectName objectName = (ObjectName) o;

        return new EqualsBuilder() //
            .append(getCatalogName(), objectName.getCatalogName()) //
            .append(getSchemaName(), objectName.getSchemaName()) //
            .append(getTableName(), objectName.getTableName()) //
            .isEquals();
    }

    public String getCatalogName() {
        return catalogName;
    }

    public String getSchemaName() {
        return schemaName;
    }

    public String getTableName() {
        return tableName;
    }

    @Override
    public int hashCode() {
        return new HashCodeBuilder(17, 37) //
            .append(getCatalogName()) //
            .append(getSchemaName()) //
            .append(getTableName()) //
            .toHashCode();
    }

    @Override
    public String toString() {
        List<String> tokensList = new ArrayList<>();
        FunctionHelper.executeIfNotNull(StringUtils.trimToNull(getCatalogName()), tokensList::add);
        FunctionHelper.executeIfNotNull(StringUtils.trimToNull(getSchemaName()), tokensList::add);
        FunctionHelper.executeIfNotNull(StringUtils.trimToNull(getTableName()), tokensList::add);
        return String.join(".", tokensList);
    }
}
