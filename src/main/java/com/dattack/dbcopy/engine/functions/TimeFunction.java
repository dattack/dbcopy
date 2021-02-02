/*
 * Copyright (c) 2020, The Dattack team (http://www.dattack.com)
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
import com.dattack.dbcopy.engine.datatype.TimeType;

import java.sql.ResultSet;
import java.sql.SQLException;

/**
 * @author cvarela
 * @since 0.3
 */
public class TimeFunction extends AbstractDataFunction<TimeType> {

    public TimeFunction(ColumnMetadata columnMetadata) {
        super(columnMetadata);
    }

    @Override
    public TimeType doGet(ResultSet rs, int index) throws SQLException {
        return new TimeType(rs.getTime(index));
    }

    @Override
    public void accept(FunctionVisitor visitor) throws Exception {
        visitor.visit(this);
    }

    @Override
    TimeType getNull() {
        return TimeType.NULL;
    }
}
