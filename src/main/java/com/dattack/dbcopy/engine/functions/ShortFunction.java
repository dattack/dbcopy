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
import com.dattack.dbcopy.engine.datatype.ShortType;
import java.sql.ResultSet;
import java.sql.SQLException;

/**
 * {@link AbstractDataFunction} implementation to retrieve values of type {@link ShortType}.
 *
 * @author cvarela
 * @since 0.3
 */
public class ShortFunction extends AbstractDataFunction<ShortType> {

    public ShortFunction(final ColumnMetadata columnMetadata) {
        super(columnMetadata);
    }

    @Override
    public void accept(final FunctionVisitor visitor) throws FunctionException {
        visitor.visit(this);
    }

    @Override
    protected ShortType doGet(final ResultSet rs, final int index) throws SQLException {
        return new ShortType(rs.getShort(index));
    }

    @Override
    protected ShortType getNull() {
        return ShortType.NULL;
    }
}
