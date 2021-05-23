/*
 * Copyright (c) 2021, The Dattack team (http://www.dattack.com)
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
package com.dattack.dbcopy.engine.functions;

import com.dattack.dbcopy.engine.ColumnMetadata;
import com.dattack.dbcopy.engine.datatype.AbstractDataType;
import java.sql.ResultSet;
import java.sql.SQLException;

/**
 * Definition of a function to retrieve a value from a ResultSet of a given data type.
 *
 * @author cvarela
 * @since 0.3
 */
public abstract class AbstractDataFunction<T extends AbstractDataType<?>> {

    private final ColumnMetadata columnMetadata;

    public AbstractDataFunction(final ColumnMetadata columnMetadata) {
        this.columnMetadata = columnMetadata;
    }

    public abstract void accept(FunctionVisitor visitor) throws Exception;

    /**
     * Returns the value of the ResultSet corresponding to the column on which this function is executed.
     *
     * @param rs the ResultSet object
     * @return the value of the ResultSet corresponding to the column on which this function is executed.
     * @throws SQLException if a database access error occurs or this method is called on a closed result set.
     */
    public T get(final ResultSet rs) throws SQLException {
        T result = doGet(rs, columnMetadata.getIndex());
        if (rs.wasNull()) {
            result = getNull();
        }
        return result;
    }

    /* default */ abstract T doGet(ResultSet rs, int index) throws SQLException;

    /**
     * Returns the representation of the NULL value for this data type.
     *
     * @return the representation of the NULL value for this data type.
     */
    protected abstract T getNull();

    public ColumnMetadata getColumnMetadata() {
        return columnMetadata;
    }
}
