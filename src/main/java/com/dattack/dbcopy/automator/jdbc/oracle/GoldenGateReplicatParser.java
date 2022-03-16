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

import com.dattack.dbcopy.automator.ReplicationConfiguration;
import com.dattack.dbcopy.automator.ReplicationConfigurationParser;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang.StringUtils;

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Parser of an Oracle GoldenGate replicat configuration file.
 *
 * <p>Syntax for MAP:</p>
 *
 * <pre>
 *     MAP source_table, TARGET target_table
 * [, COLMAP (column_mapping)]
 * [, COMPARECOLS (column_specification)]
 * [, COORDINATED]
 * [, {DEF | TARGETDEF} template]
 * [, EXCEPTIONSONLY]
 * [, EXITPARAM 'parameter']
 * [, EVENTACTIONS (action)]
 * [, FILTER (filter_clause)]
 * [, HANDLECOLLISIONS | NOHANDLECOLLISIONS]
 * [, INSERTALLRECORDS]
 * [, INSERTAPPEND | NOINSERTAPPEND]
 * [, KEYCOLS (columns)]
 * [, MAPEXCEPTION (exceptions_mapping)]
 * [, REPERROR (error, response)]
 * [, RESOLVECONFLICT (conflict_resolution_specification)]
 * [, SQLEXEC (SQL_specification)]
 * [, THREAD (thread_ID)]
 * [, THREADRANGE (thread_range[, column_list])]
 * [, TRIMSPACES | NOTRIMSPACES]
 * [, TRIMVARSPACES | NOTRIMVARSPACES]
 * [, WHERE (clause)]
 * ;
 * </pre>
 */
public class GoldenGateReplicatParser implements ReplicationConfigurationParser {

    @Override
    public ReplicationConfiguration parse(Path path) throws IOException {
        InputStream inputStream = Files.newInputStream(path);
        String text = IOUtils.toString(inputStream, StandardCharsets.UTF_8);
        text = text.replaceAll("--.*\n", ""); // remove inline comments
        text = text.replaceAll("\n", "");

        String sourceTable = getTokenValue("MAP", text);
        String targetTable = getTokenValue("TARGET", text);

        ReplicationConfiguration replicationConfiguration = null;
        if (StringUtils.isNotEmpty(sourceTable) && StringUtils.isNotEmpty(targetTable)) {
            replicationConfiguration = new ReplicationConfiguration(sourceTable, targetTable);
            text = text.replaceAll("\\s+", "");
            text = text.replaceAll("\\('|'\\)|'\\s*,\\s*'|\\(\\)", "_");
            appendColMap(replicationConfiguration, text);
        }
        return replicationConfiguration;
    }

    private static String getTokenValue(String tokenName, String text) {
        Pattern pattern = Pattern.compile(".*" + tokenName + "\\s+(\\w+(\\.?\\w)+).*", Pattern.CASE_INSENSITIVE);
        return getFirstGroup(text, pattern);
    }

    private static void appendColMap(ReplicationConfiguration replicationConfiguration, String text) {

        if (replicationConfiguration == null || text == null) {
            return;
        }

        Pattern pattern = Pattern.compile(".*COLMAP\\s*\\(([^)]+)\\).*", Pattern.CASE_INSENSITIVE);
        String colMap = getFirstGroup(text, pattern);
        if (colMap != null) {
            for (String token : colMap.split(",")) {
                String[] colMapping = token.split("=");
                if (colMapping.length == 2) {
                    replicationConfiguration.addMapping(cleanup(colMapping[0]), cleanup(colMapping[1]));
                }
            }
        }
    }

    private static String cleanup(String value) {
        return value.replaceAll("_+$", "");
    }

    private static String getFirstGroup(String text, Pattern pattern) {
        Matcher matcher = pattern.matcher(text);
        String result = null;
        if (matcher.matches()) {
            result = matcher.group(1);
        }
        return result;
    }
}
