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
import com.dattack.dbcopy.automator.TableMapping;
import com.dattack.dbcopy.automator.ObjectName;
import org.apache.commons.lang3.StringUtils;

import java.util.Map;
import java.util.Objects;
import java.util.Properties;
import java.util.stream.Collectors;

import static com.dattack.dbcopy.automator.CodeHelper.NTAB2;
import static com.dattack.dbcopy.automator.CodeHelper.NTAB3;
import static com.dattack.dbcopy.automator.CodeHelper.TAB3;
import static com.dattack.dbcopy.automator.CodeHelper.getVariable;
import static com.dattack.dbcopy.automator.CodeHelper.reformat;

public class DefaultInsertStrategy implements InsertStrategy {

    private static final String UNKNOWN_COLUMN =
        "null    /*   * * * * *   WARNING: the column %s does not exists in the source table   * * * * *   */";

    private static final String UNKNOWN_FUNCTION =
        "%s    /*   * * * * *   WARNING: the expression %s is an unknown function   * * * * *   */";

    @Override
    public String execute(ObjectName tableName, TableMapping mapping) {
        return "INSERT INTO " + tableName + " (" + NTAB3 + reformat(getTargetFieldList(mapping), TAB3) //
            + NTAB2 + ") VALUES (" + NTAB3 + reformat(getSourceFieldList(mapping), TAB3) //
            + NTAB2 + ")";
    }

    protected String getSourceFieldList(TableMapping mapping) {
        return mapping.getColumnMappingList().stream() //
            .map(s -> { //
                if (s.getSourceColumn() == null) {

                    return getGlobalMapping(mapping.getGlobalMapping(), s.getTargetColumn().getName(),
                                            String.format(UNKNOWN_COLUMN, s.getTargetColumn().getName()));
                } else if (s.getSourceColumn().getOrdinalPosition() >= 0) {
                    return getVariable(s.getSourceColumn().getName());
                } else {
                    return getGlobalMapping(mapping.getGlobalMapping(), s.getSourceColumn().getName(),
                        String.format(UNKNOWN_FUNCTION, s.getSourceColumn().getName(), s.getSourceColumn().getName()));
                }
            }) //
            .collect(Collectors.joining(",\n")) //
            ;
    }

    protected String getTargetFieldList(TableMapping mapping) {
        return mapping.getColumnMappingList().stream() //
            .map(s -> s.getTargetColumn().getName()) //
            .collect(Collectors.joining(",\n")) //
            ;
    }

    private String getGlobalMapping(Map<?, ?> properties, String key, String defaultValue) {
        String result;
        if (properties != null) {
            result = Objects.toString(properties.get(key), defaultValue);
        } else {
            result = defaultValue;
        }
        return result;
    }
}
