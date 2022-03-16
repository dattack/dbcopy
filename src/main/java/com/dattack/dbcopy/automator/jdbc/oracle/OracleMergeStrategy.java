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

import com.dattack.dbcopy.automator.TableMapping;
import com.dattack.dbcopy.automator.ObjectName;
import com.dattack.dbcopy.automator.jdbc.DefaultInsertStrategy;

import java.util.stream.Collectors;

import static com.dattack.dbcopy.automator.CodeHelper.NTAB1;
import static com.dattack.dbcopy.automator.CodeHelper.NTAB2;
import static com.dattack.dbcopy.automator.CodeHelper.NTAB3;
import static com.dattack.dbcopy.automator.CodeHelper.TAB3;
import static com.dattack.dbcopy.automator.CodeHelper.reformat;
import static com.dattack.dbcopy.automator.CodeHelper.getVariable;

/**
 * An {@link DefaultInsertStrategy} extension that uses a MERGE sentence instead of the classical INSERT.
 *
 * @author cvarela
 * @since 0.3
 */
public class OracleMergeStrategy extends DefaultInsertStrategy {

    @Override
    public String execute(ObjectName tableName, TableMapping mapping) {
        return "MERGE INTO " + tableName //
                + NTAB1 + "USING dual ON (" + NTAB3 + reformat(getKeyFieldMapping(mapping), TAB3) + NTAB3 + ")" //
                + NTAB1 + "WHEN MATCHED THEN UPDATE SET" + NTAB3 + reformat(getNonKeyFieldMapping(mapping), TAB3) //
                + NTAB1 + "WHEN NOT MATCHED THEN INSERT (" + NTAB3 + reformat(getTargetFieldList(mapping), TAB3) //
                + NTAB2 + ") VALUES (" + NTAB3 + reformat(getSourceFieldList(mapping), TAB3) //
                + NTAB2 + ")";
    }

    private String getKeyFieldMapping(TableMapping mapping) {
        return getFieldMapping(mapping, true, " AND ");
    }

    private String getNonKeyFieldMapping(TableMapping mapping) {
        return getFieldMapping(mapping, false, ",\n");
    }

    private String getFieldMapping(TableMapping tableMapping, boolean isKey, String delimiter) {
        return tableMapping.getColumnMappingList().stream()
                .filter(s -> s.getTargetColumn().isPrimaryKey() == isKey)
                .map(s -> s.getTargetColumn().getName() + " = " + getVariable(s.getSourceColumn().getName()))
                .collect(Collectors.joining(delimiter));
    }
}
